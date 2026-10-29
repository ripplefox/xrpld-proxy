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
        maxConcurrency: 5,
        currentIndex: 0, // 用于平滑轮询 (Round Robin)
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
        const availableNodes = cluster.nodes.filter(n => n.status === 'Online');
        if (availableNodes.length === 0) return null;

        for (let i = 0; i < cluster.nodes.length; i++) {
            const idx = (cluster.currentIndex + i) % cluster.nodes.length;
            const node = cluster.nodes[idx];
            if (node.status === 'Online' && node.activeRequests < cluster.maxConcurrency) {
                cluster.currentIndex = (idx + 1) % cluster.nodes.length;
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
        // 无有效 ID 和 Command 时，直接对齐 XRPL 的 jsonInvalid 报错规范
        return sendXrpldError(ws, undefined, undefined, "jsonInvalid", "Your request could not be parsed as valid JSON.", 90);
    }

    const command = payload.command;
    const originalId = payload.id;
    
    if (!command) {
        return sendXrpldError(ws, originalId, undefined, "missingCommand", "The request does not contain a command field.", 91);
    }

    const isPathFind = (command === 'path_find' || command === 'ripple_path_find');
    const clusterKey = isPathFind ? 'pathFind' : 'submit';
    
    const selectedNode = selectNode(command, clusterKey);
    if (!selectedNode) {
        // 【集群全满载】：触发 busy 错，完全对齐 XRPLD 原生大流量下的 slowDown / amtaNoNodes 机制并透传 busy
        return sendXrpldError(ws, originalId, command, "busy", "All backend nodes are saturated or offline. Please retry later.", 88);
    }

    internalIdCounter++;
    const prefix = isPathFind ? 'pf_' : 'sub_';
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
                
                // 超时强制清退：返回 XRPL 标准的 timeout 错
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
    selectedNode.activeRequests++;
    
    try {
        selectedNode.client.send(JSON.stringify(payload));
    } catch (err) {
        selectedNode.activeRequests = Math.max(0, selectedNode.activeRequests - 1);
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

    const internalId = payload.id;
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
            
            // 超限熔断：返回符合规范的 tooBusy / excessiveUpdates 错误
            sendXrpldError(ctx.ws, ctx.originalId, ctx.command, "tooBusy", "Path find updates exceeded maximum gateway buffer threshold (20 limit).", 89);
            requestMap.delete(internalId);
            return;
        }
    }

    payload.id = ctx.originalId;
    
    try {
        if (ctx.ws.readyState === WebSocket.OPEN) {
            ctx.ws.send(JSON.stringify(payload));
        }
    } catch (e) {}

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
    
    // 同时支持 /status 和 / 根路径路由访问监控看板
    if (parsedUrl.pathname === '/status' || parsedUrl.pathname === '/') {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        
        let pClusterStr = '';
        clusters.pathFind.nodes.forEach(n => {
            const statusLabel = n.status === 'Online' ? '[● Online]' : (n.status === 'Isolated' ? '[⚠️ Isolated]' : '[○ Offline]');
            const progress = '■'.repeat(n.activeRequests) + ' '.repeat(clusters.pathFind.maxConcurrency - n.activeRequests);
            pClusterStr += `  - ${n.url.padEnd(25)} ${statusLabel.padEnd(13)} Load: [${progress}] ${n.activeRequests}/${clusters.pathFind.maxConcurrency}\n`;
        });

        let sClusterStr = '';
        clusters.submit.nodes.forEach(n => {
            const statusLabel = n.status === 'Online' ? '[● Online]' : (n.status === 'Isolated' ? '[⚠️ Isolated]' : '[○ Offline]');
            const progress = '■'.repeat(n.activeRequests) + ' '.repeat(clusters.submit.maxConcurrency - n.activeRequests);
            sClusterStr += `  - ${n.url.padEnd(25)} ${statusLabel.padEnd(13)} Load: [${progress}] ${n.activeRequests}/${clusters.submit.maxConcurrency}\n`;
        });

        // 扁平化无框看板设计
        const view = 
`========================================================
 XRPLD LB CLUSTER STATUS
========================================================
 Active Frontend Clients: ${activeFrontendClients}

 [Path Find Cluster]
${pClusterStr}
 [Submit Cluster]
${sClusterStr}========================================================\n`;
        
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
        // 清理该前端残留的未决映射（防止因前端单向断流导致的网关计数和并发名额被僵尸请求长期霸占）
        for (const [internalId, ctx] of requestMap.entries()) {
            if (ctx.ws === ws) {
                clearTimeout(ctx.timer);
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

