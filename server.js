const WebSocket = require('ws');
const http = require('http');
const url = require('url');

// ==========================================
// 1. 配置与集群初始化
// ==========================================
const PORT = process.env.PORT || 3000;

const clusters = {
    pathFind: {
        maxConcurrency: 2,
        nodes: [
            { url: 'wss://xrplcluster.com', status: 'Offline', activeRequests: 0, failCount: 0, client: null },
            { url: 'wss://xrpl.ws', status: 'Offline', activeRequests: 0, failCount: 0, client: null },
            { url: 'wss://xrpl.link', status: 'Offline', activeRequests: 0, failCount: 0, client: null }
        ]
    },
    submit: {
        maxConcurrency: 6,
        nodes: [
            { url: 'wss://s1.ripple.com', status: 'Offline', activeRequests: 0, failCount: 0, client: null },
            { url: 'wss://s2.ripple.com', status: 'Offline', activeRequests: 0, failCount: 0, client: null },
            { url: 'wss://honeycluster.io', status: 'Offline', activeRequests: 0, failCount: 0, client: null }
        ]
    }
};

// 全局上下文状态维护
let activeFrontendClients = 0;
let internalIdCounter = 0;
// 内部唯一ID -> { ws: 前端Socket, originalId: 原始ID, command: 原始命令, node: 后端节点, timer: 生命周期定时器, streamCount: 计数 }
const requestMap = new Map(); 

// ==========================================
// 核心工具函数：对齐 XRPLD 规范的标准报错分发器
// ==========================================
function sendXrpldError(ws, originalId, command, errCodeStr, errMessage, numericCode = 99) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    
    // 严格参照 XRPLD 的 API 错误响应格式
    const errorPayload = {
        id: originalId,
        status: "error",
        type: "response",
        error: errCodeStr,
        error_code: numericCode,
        error_message: errMessage,
        request: {
            command: command || "unknown"
        }
    };
    try {
        console.log(`[RES_ERR] ID: ${originalId} -> ${errMessage}`);
        ws.send(JSON.stringify(errorPayload));
    } catch (e) {}
}

// ==========================================
// 2. 后端集群连接与自愈管理 (Circuit Breaker)
// ==========================================
function initBackendCluster() {
    Object.keys(clusters).forEach(clusterKey => {
        clusters[clusterKey].nodes.forEach(node => {
            connectToNode(node, clusterKey);
        });
    });
}

function connectToNode(node, clusterKey) {
    if (node.client) {
        try { node.client.terminate(); } catch (e) {}
    }

    const ws = new WebSocket(node.url);
    node.client = ws;

    ws.on('open', () => {
        node.status = 'Online';
        node.failCount = 0;
    });

    ws.on('message', (data) => {
        handleBackendMessage(data, node);
    });

    ws.on('close', () => {
        node.status = 'Offline';
        // 【自愈机制 4：断线自动归零锁】
        node.activeRequests = 0;
        cleanupMapForNode(node);
        setTimeout(() => connectToNode(node, clusterKey), 5000); 
    });

    ws.on('error', (err) => {
        handleNodeFailure(node, clusterKey);
    });
}

function handleNodeFailure(node, clusterKey) {
    if (node.status === 'Isolated') return;
    
    node.failCount++;
    if (node.failCount >= 3) {
        node.status = 'Isolated';
        node.activeRequests = 0;
        cleanupMapForNode(node);
    }
}

function cleanupMapForNode(node) {
    for (const [internalId, ctx] of requestMap.entries()) {
        if (ctx.node === node) {
            clearTimeout(ctx.timer);
            // 后端断线导致的异常：返回 XRPL 标准的 disconnected 错误
            sendXrpldError(ctx.ws, ctx.originalId, ctx.command, "disconnected", "The backend node processing this request disconnected.", 1001);
            requestMap.delete(internalId);
        }
    }
}

// 后台熔断自愈探针
setInterval(() => {
    Object.keys(clusters).forEach(clusterKey => {
        clusters[clusterKey].nodes.forEach(node => {
            if (node.status === 'Isolated' && node.client && node.client.readyState === WebSocket.OPEN) {
                node.client.send(JSON.stringify({ id: "gateway_probe_" + Math.random(), command: "ping" }));
                node.status = 'Online';
                node.failCount = 0;
            }
        });
    });
}, 10000);

// ==========================================
// 3. 核心分流与负载均衡路由算法
// ==========================================
function selectNode(command, clusterKey) {
    const cluster = clusters[clusterKey];
    
    if (clusterKey === 'pathFind') {
        let targetNode = null;
        let minLoad = cluster.maxConcurrency;
        
        for (const node of cluster.nodes) {
            if (node.status === 'Online' && node.activeRequests < minLoad) {
                minLoad = node.activeRequests;
                targetNode = node;
            }
        }
        return targetNode;
    }
    
    if (clusterKey === 'submit') {
        // 【算法改进】：顺序填充策略（客户端优先连接同一个节点，直到满载才切换下一个）
        for (let i = 0; i < cluster.nodes.length; i++) {
            const node = cluster.nodes[i];
            // 只要节点在线，且当前的并发请求数还没有达到设定的上限，就直接锁定该节点
            if (node.status === 'Online' && node.activeRequests < cluster.maxConcurrency) {
                return node;
            }
        }
    }
    return null;
}

// ==========================================
// 4. 全双工业务数据处理与自愈实现
// ==========================================
function handleClientRequest(ws, rawData) {
    let payload;
    try {
        payload = JSON.parse(rawData);
    } catch (e) {
        return sendXrpldError(ws, undefined, undefined, "jsonInvalid", "Your request could not be parsed as valid JSON.", 90);
    }

    const command = payload.command;
    const originalId = payload.id;
    
    if (!command) {
        return sendXrpldError(ws, originalId, undefined, "missingCommand", "The request does not contain a command field.", 91);
    }

    // 识别特殊的特殊流式广播命令：判断是否为 ledger 订阅
    const isPathFind = (command === 'path_find' || command === 'ripple_path_find');
    const isLedgerSubscribe = (command === 'subscribe' && payload.streams && payload.streams.includes('ledger'));
    
    const clusterKey = isPathFind ? 'pathFind' : 'submit';
    
    // 路由选点
    const selectedNode = selectNode(command, clusterKey);
    if (!selectedNode) {
        return sendXrpldError(ws, originalId, command, "busy", "All backend nodes are saturated or offline. Please retry later.", 88);
    }

    // 会话 ID 动态映射
    internalIdCounter++;
    let prefix = 'sub_';
    if (isPathFind) prefix = 'pf_';
    if (isLedgerSubscribe) prefix = 'led_'; // 赋予 ledger 订阅专属前缀
    
    const internalId = `${prefix}${internalIdCounter}`;

    console.log(`[ROUTE] Command: ${command} | Client ID: ${originalId} -> Internal ID: ${internalId} | Target: ${selectedNode.url}`);

    let lifecycleTimer = null;
    if (isPathFind) {
        // 【自愈机制 3：绝对生命周期窗口 3分钟】
        lifecycleTimer = setTimeout(() => {
            if (requestMap.has(internalId)) {
                console.log(`[ANTI-DEADLOCK] Absolute lifecycle timeout for ${internalId}. Evicting.`);
                if (selectedNode.client && selectedNode.client.readyState === WebSocket.OPEN) {
                    selectedNode.client.send(JSON.stringify({ command: "path_find", sub_command: "close" }));
                }
                selectedNode.activeRequests = Math.max(0, selectedNode.activeRequests - 1);
                sendXrpldError(ws, originalId, command, "timeout", "Gateway forcefully closed the long-lived stream after 3 minutes limit.", 408);
                requestMap.delete(internalId);
            }
        }, 180000); 
    }

    requestMap.set(internalId, {
        ws: ws,
        originalId: originalId,
        command: command,
        node: selectedNode,
        timer: lifecycleTimer,
        streamCount: 0
    });

    payload.id = internalId;

    // 【修改点】：如果不是 ledger 订阅，才占用节点的并发计数器
    if (!isLedgerSubscribe) {
        selectedNode.activeRequests++;
    } else {
        console.log(`[BYPASS] Ledger subscription bypasses concurrency counters for ${selectedNode.url}`);
    }
    
    try {
        selectedNode.client.send(JSON.stringify(payload));
    } catch (err) {
        if (!isLedgerSubscribe) {
            selectedNode.activeRequests = Math.max(0, selectedNode.activeRequests - 1);
        }
        requestMap.delete(internalId);
        if (lifecycleTimer) clearTimeout(lifecycleTimer);
        handleNodeFailure(selectedNode, clusterKey);
        sendXrpldError(ws, originalId, command, "internalError", "Gateway failed to proxy the request downstream.", 500);
    }
}

function handleBackendMessage(rawData, node) {
    let payload;
    try {
        payload = JSON.parse(rawData);
    } catch (e) { return; }

    // 针对 XRPL 异步 ledger 广播推送包（通常无请求 ID 或返回类型为 ledgerClosed）进行上下文匹配
    let internalId = payload.id;
    
    // 如果是后端主动分发的无 ID 的 ledger 广播，需要全量广播给订阅了该节点的客户端
    if (!internalId && payload.type === "ledgerClosed") {
        for (const [id, ctx] of requestMap.entries()) {
            if (id.startsWith('led_') && ctx.node === node && ctx.ws.readyState === WebSocket.OPEN) {
                try { ctx.ws.send(rawData); } catch(e){}
            }
        }
        return;
    }

    if (!internalId || !requestMap.has(internalId)) return;
    const ctx = requestMap.get(internalId);
    
    console.log(`[RESPONSE] Internal ID: ${internalId} -> Client ID: ${ctx.originalId}`);

    // 【自愈机制 2：流式报文超限截断】
    if (internalId.startsWith('pf_')) {
        ctx.streamCount++;
        if (ctx.streamCount > 20) {
            console.log(`[ANTI-DEADLOCK] Flow stream ${internalId} exceeded 20 updates. Force terminating.`);
            if (node.client && node.client.readyState === WebSocket.OPEN) {
                node.client.send(JSON.stringify({ command: "path_find", sub_command: "close" }));
            }
            node.activeRequests = Math.max(0, node.activeRequests - 1);
            clearTimeout(ctx.timer);
            sendXrpldError(ctx.ws, ctx.originalId, ctx.command, "tooBusy", "Path find updates exceeded maximum gateway buffer threshold (20 limit).", 89);
            requestMap.delete(internalId);
            return;
        }
    }

    // 还原原始客户端请求 ID 并回传
    payload.id = ctx.originalId;
    
    try {
        if (ctx.ws.readyState === WebSocket.OPEN) {
            ctx.ws.send(JSON.stringify(payload));
        }
    } catch (e) {}

    // 【修改点】：普通命令正常释放计数与映射；ledger 订阅（led_）属于长连接流，初次订阅成功回包后也“不释放映射”，持续等待推送
    if (internalId.startsWith('sub_')) {
        node.activeRequests = Math.max(0, node.activeRequests - 1);
        requestMap.delete(internalId);
    }
}

// ==========================================
// 5. 单端口复用服务器 (HTTP + WebSocket)
// ==========================================
const server = http.createServer((req, res) => {
    const parsedUrl = url.parse(req.url, true);
    
    if (parsedUrl.pathname === '/status' || parsedUrl.pathname === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        
        // 提取通用的节点状态看板渲染逻辑
        const renderCluster = (nodes, maxConcurrency) => {
            let str = '';
            nodes.forEach(n => {
                let statusLabel = '[○ Offline]';
                if (n.status === 'Online') {
                    // 如果是在线状态，进一步区分是否已经占满连接
                    statusLabel = n.activeRequests >= maxConcurrency ? '[🔥 FULL]   ' : '[● Online]  ';
                } else if (n.status === 'Isolated') {
                    statusLabel = '[⚠️ Isolated]';
                }
                
                // 计算负载百分比并绘制进度条
                const usagePercent = Math.round((n.activeRequests / maxConcurrency) * 100);
                const progress = '■'.repeat(n.activeRequests) + ' '.repeat(maxConcurrency - n.activeRequests);
                
                str += `  - ${n.url.padEnd(25)} ${statusLabel} 负载: [${progress}] ${String(n.activeRequests).padStart(2)}/${maxConcurrency} (${String(usagePercent).padStart(3)}%)\n`;
            });
            return str;
        };

        const pClusterStr = renderCluster(clusters.pathFind.nodes, clusters.pathFind.maxConcurrency);
        const sClusterStr = renderCluster(clusters.submit.nodes, clusters.submit.maxConcurrency);

        // 现代化、更具可读性的无框看板设计
        const view = 
`======================================================================
 XRPLD REVERSE PROXY & LOAD BALANCER STATUS
======================================================================
 [Gateway Telemetry]
  - Active Frontend Clients : ${activeFrontendClients}
  - Total Memory Mapping Contexts : ${requestMap.size}

 [Path Find Cluster] (Strategy: Minimum Load Available)
${pClusterStr}
 [Submit Cluster] (Strategy: Linear Fill / First Available)
${sClusterStr}======================================================================\n`;
        
        res.end(view);
    } else {
        res.writeHead(404);
        res.end();
    }
});

// 接入 WebSocket 服务器 (不独立监听端口，交由上方 HTTP 实例统一调度管理)
const wss = new WebSocket.Server({ noServer: true });

// 监听单端口协议升级事件，将 WebSocket 握手请求交由 wss 处理
server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

wss.on('connection', (ws) => {
    activeFrontendClients++;
    
    // 【自愈机制 1：前端 TCP 假死心跳剔除初始化】
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (message) => {
        handleClientRequest(ws, message);
    });

    ws.on('close', () => {
        activeFrontendClients = Math.max(0, activeFrontendClients - 1);
        
        // 遍历所有映射关系，清理该前端残留的未决映射与底层订阅流
        for (const [internalId, ctx] of requestMap.entries()) {
            if (ctx.ws === ws) {
                clearTimeout(ctx.timer);
                
                // === 【核心修复代码：向后端节点同步下发退订指令】 ===
                if (ctx.node && ctx.node.client && ctx.node.client.readyState === WebSocket.OPEN) {
                    if (internalId.startsWith('pf_')) {
                        if (ctx.command === 'subscribe') {
                            // 针对订阅请求（例如 ledger 账本流），在前端断开时显式通知对应的下游节点退订
                            try {
                                ctx.node.client.send(JSON.stringify({
                                    id: internalId,
                                    command: "unsubscribe",
                                    streams: ["ledger", "accounts"] // 强制解除最常见的长轮询基础流
                                }));
                                console.log(`[UNSUBSCRIBE] Automatically clean up subscription stream for internal ID: ${internalId}`);
                            } catch (e) {
                                console.error(`[ERR] Failed to send unsubscribe down to node for ${internalId}`, e);
                            }
                        } else {
                            // 针对普通的 path_find 路径查找长连接进行关闭
                            try {
                                ctx.node.client.send(JSON.stringify({ 
                                    id: internalId,
                                    command: "path_find", 
                                    sub_command: "close" 
                                }));
                            } catch (e) {
                                console.error(`[ERR] Failed to close path_find downstream for ${internalId}`, e);
                            }
                        }
                    }
                }
                // === 【修复结束】 ===

                // 释放该连接占用的节点并发名额并安全移出内存映射
                ctx.node.activeRequests = Math.max(0, ctx.node.activeRequests - 1);
                requestMap.delete(internalId);
            }
        }
    });
});

// 【心跳自愈轮询】：网关每 30 秒发起一次物理 Ping
const heartBeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log(`[ANTI-DEADLOCK] Frontend TCP silent dead-link detected. Terminating connection.`);
            return ws.terminate(); // 连续 30 秒失联无 Pong 响应，强行 terminate 物理连接并触发 close 回收指标
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(heartBeatInterval);
});
// 启动网关服务
server.listen(PORT, () => {
    console.log(`[GATEWAY] XRPLD Balance Proxy running on port ${PORT}`);
    initBackendCluster(); // 初始化下游 XRPL 节点集群连接
});