const http = require('http');
const WebSocket = require('ws');

// ==========================================
// 1. 全局配置与状态定义
// ==========================================
const PORT = 3000;
const MAX_CONCURRENCY = 3;           // 每台后端最大并发数
const HEALTH_CHECK_INTERVAL = 30000; // 30秒健康检查一次
const HEALTH_SUCCESS_THRESHOLD = 2;  // 连续成功2次后上线
const REQUEST_TIMEOUT = 10000;        // 客户端请求超时时间：10秒

let frontendClientsCount = 0;
let currentIndex = 0;

// 节点池状态管理
const nodes = [
    { url: 'wss://xrplcluster.com', status: 'Offline', currentLoad: 0, poolWs: null, successCount: 0 },
    { url: 'wss://s1.ripple.com', status: 'Offline', currentLoad: 0, poolWs: null, successCount: 0 },
    { url: 'wss://s2.ripple.com', status: 'Offline', currentLoad: 0, poolWs: null, successCount: 0 },
    { url: 'wss://honeycluster.io', status: 'Offline', currentLoad: 0, poolWs: null, successCount: 0 }
];

// 请求回执回调映射表（用于长连接多路复用响应路由）
const responseCallbacks = new Map();

// ==========================================
// 2. 长连接池初始化与管理
// ==========================================
function initNodeConnection(node) {
    if (node.poolWs) {
        try { node.poolWs.terminate(); } catch(e) {}
    }

    console.log(`[Pool] Connecting to backend: ${node.url}`);
    const ws = new WebSocket(node.url);
    node.poolWs = ws;

    ws.on('open', () => {
        console.log(`[Pool] TCP Link established with ${node.url}. Waiting for health check to online...`);
    });

    ws.on('message', (data) => {
        try {
            const res = JSON.parse(data);
            
            // 场景 A：标准请求-响应（带 id）
            if (res.id && responseCallbacks.has(res.id)) {
                const callbackInfo = responseCallbacks.get(res.id);
                responseCallbacks.delete(res.id);
                clearTimeout(callbackInfo.timeoutId);
                callbackInfo.callback(null, data);
                return;
            }

            // 场景 B：XRPL 节点主动推送的订阅事件（不通过 id 匹配，而是通过事件特征广播/定向分发）
            // XRPL 订阅推送的 JSON 特征：含有 type 字段（如 "ledgerClosed", "transaction" 等）
            if (res.type) {
                broadcastSubscribeEvent(node.url, res.type, data);
            }

        } catch (err) {
            console.error(`[Pool] Error parsing message from ${node.url}:`, err);
        }
    });

    ws.on('error', (err) => {
        console.error(`[Pool] Connection error on ${node.url}:`, err.message);
        triggerCircuitBreaker(node, 'Offline');
    });

    ws.on('close', () => {
        console.warn(`[Pool] Connection closed by ${node.url}. Reconnecting in 15s...`);
        triggerCircuitBreaker(node, 'Offline');
        setTimeout(() => initNodeConnection(node), 15000);
    });
}

function triggerCircuitBreaker(node, targetStatus = 'Isolated') {
    if (node.status !== targetStatus) {
        console.warn(`[CircuitBreaker] 🚨 Node ${node.url} enters [${targetStatus}] state.`);
        if (targetStatus === 'Offline' || targetStatus === 'Isolated') {
            node.currentLoad = 0;
        }
    }
    node.status = targetStatus;
    node.successCount = 0; 
}

nodes.forEach(node => initNodeConnection(node));

// ==========================================
// 3. 动态健康检查
// ==========================================
setInterval(() => {
    nodes.forEach(node => {
        if (!node.poolWs || node.poolWs.readyState !== WebSocket.OPEN) {
            triggerCircuitBreaker(node, 'Offline');
            return;
        }

        const checkId = `health_check_${Date.now()}_${Math.random()}`;
        let timeout = setTimeout(() => {
            responseCallbacks.delete(checkId);
            triggerCircuitBreaker(node, 'Isolated');
        }, 5000);

        responseCallbacks.set(checkId, {
            timeoutId: timeout,
            callback: (err, data) => {
                if (err) { triggerCircuitBreaker(node, 'Isolated'); return; }
                try {
                    const res = JSON.parse(data);
                    const state = res.result?.state?.server_state;
                    if (state === 'full' || state === 'validating') {
                        node.successCount++;
                        if (node.successCount >= HEALTH_SUCCESS_THRESHOLD) {
                            node.status = 'Online';
                        }
                    } else {
                        triggerCircuitBreaker(node, 'Isolated');
                    }
                } catch (e) {
                    triggerCircuitBreaker(node, 'Isolated');
                }
            }
        });

        try {
            node.poolWs.send(JSON.stringify({ id: checkId, command: "server_state" }));
        } catch (e) {
            clearTimeout(timeout);
            responseCallbacks.delete(checkId);
            triggerCircuitBreaker(node, 'Offline');
        }
    });
}, HEALTH_CHECK_INTERVAL);

// ==========================================
// 4. 负载均衡算法
// ==========================================
function getNextAvailableNode() {
    const availableNodes = nodes.filter(n => n.status === 'Online' && n.currentLoad < MAX_CONCURRENCY);
    if (availableNodes.length === 0) return null;
    const node = availableNodes[currentIndex % availableNodes.length];
    currentIndex++;
    return node;
}

// ==========================================
// 5. 订阅流定向广播引擎
// ==========================================
// 维护哪些前端客户端在哪个后端节点订阅了什么流
// 格式: clientClientId -> { nodeUrl: '...', streams: Set(['ledger', 'accounts']) }
const clientSubscriptions = new Map();

function broadcastSubscribeEvent(nodeUrl, eventType, rawData) {
    // 遍历所有客户端，如果该客户端在这个节点上订阅了该类型的流，则转发
    for (const [clientWs, subInfo] of clientSubscriptions.entries()) {
        if (subInfo.nodeUrl === nodeUrl && clientWs.readyState === WebSocket.OPEN) {
            // 映射 XRPL 推送的 type 到订阅时的 streams 关键字
            // 例如：type="ledgerClosed" 对应 streams="ledger"
            const matchLedger = eventType === 'ledgerClosed' && subInfo.streams.has('ledger');
            const matchTx = eventType === 'transaction' && subInfo.streams.has('transactions');
            const matchValidation = eventType === 'validationReceived' && subInfo.streams.has('validations');
            
            // 简单的通用匹配（包含自定义 accounts 监听等）
            if (matchLedger || matchTx || matchValidation || subInfo.streams.has('accounts')) {
                clientWs.send(rawData.toString());
            }
        }
    }
}

// ==========================================
// 6. HTTP 监控看板
// ==========================================
const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/status') {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        let statusBox = `========================================================\n`;
        statusBox += `XRPLD LB CLUSTER STATUS (Auto Cleanup Enabled)\n`;
        statusBox += `========================================================\n`;
        statusBox += `Active Frontend Clients: ${frontendClientsCount}\n`;
        statusBox += `Active Subscriptions: ${clientSubscriptions.size}\n\n`;
        
        nodes.forEach(node => {
            let statusSign = '○ Offline ';
            if (node.status === 'Online') statusSign = '● Online  ';
            if (node.status === 'Isolated') statusSign = '⚠️ Isolated';
            const progress = '#'.repeat(node.currentLoad) + ' '.repeat(MAX_CONCURRENCY - node.currentLoad);
            statusBox += `- ${node.url} [${statusSign}] Load: [${progress}] ${node.currentLoad}/${MAX_CONCURRENCY}\n`;
        });
        statusBox += `========================================================\n`;
        res.end(statusBox);
    } else {
        res.writeHead(404); res.end();
    }
});

// ==========================================
// 7. WebSocket 代理网关（核心清理与多路复用逻辑）
// ==========================================
const wss = new WebSocket.Server({ server });

wss.on('connection', (clientWs) => {
    frontendClientsCount++;
    
    // 跟踪当前客户端发送的、尚未收到响应的活跃请求及其关联的节点
    // 格式: reqId -> targetNodeObject
    const clientActiveRequests = new Map();

    clientWs.on('message', (message) => {
        let parsed;
        try {
            parsed = JSON.parse(message);
        } catch (e) {
            clientWs.send(JSON.stringify({ error: "bad_request", message: "Invalid JSON metadata." }));
            return;
        }

        const reqId = parsed.id;
        if (!reqId) {
            clientWs.send(JSON.stringify({ error: "missing_id", message: "The 'id' field is required." }));
            return;
        }

        // 拦截并解析 XRPL 订阅/退订相关命令
        const isSubscribe = parsed.command === 'subscribe';
        const isUnsubscribe = parsed.command === 'unsubscribe';

        // 挑选可用后端节点（轮询 + Concurrency Control）
        const targetNode = getNextAvailableNode();
        if (!targetNode) {
            clientWs.send(JSON.stringify({ id: reqId, error: "busy", message: "All backend nodes are busy or offline." }));
            return;
        }

        // 1. 增加当前节点的并发负载计数，并将该请求记录到客户端的活跃列表中
        targetNode.currentLoad++;
        clientActiveRequests.set(reqId, targetNode);

        // 2. 设置该请求的客户端超时定时器
        const timeoutId = setTimeout(() => {
            if (responseCallbacks.has(reqId)) {
                responseCallbacks.delete(reqId);
                clientActiveRequests.delete(reqId);
                
                // 超时强制扣减负载槽位，防止死锁
                targetNode.currentLoad = Math.max(0, targetNode.currentLoad - 1);
                
                if (clientWs.readyState === WebSocket.OPEN) {
                    clientWs.send(JSON.stringify({ id: reqId, error: "timeout", message: "Request timed out." }));
                }
                console.warn(`[Timeout] Request ${reqId} forwarded to ${targetNode.url} timed out.`);
            }
        }, REQUEST_TIMEOUT);

        // 3. 注册多路复用回执回调
        responseCallbacks.set(reqId, {
            timeoutId: timeoutId,
            callback: (err, responseData) => {
                // 正常响应或发生连接断开错误时，扣减负载，并移出活跃列表
                targetNode.currentLoad = Math.max(0, targetNode.currentLoad - 1);
                clientActiveRequests.delete(reqId);
                
                if (err) {
                    if (clientWs.readyState === WebSocket.OPEN) {
                        clientWs.send(JSON.stringify({ id: reqId, error: "node_error", message: "Backend connection broken during request." }));
                    }
                    return;
                }

                // 核心业务：当确认后端节点成功接受了客户端的订阅/退订命令后，在代理层同步登记状态
                if (isSubscribe && parsed.streams) {
                    if (!clientSubscriptions.has(clientWs)) {
                        clientSubscriptions.set(clientWs, { nodeUrl: targetNode.url, streams: new Set() });
                    }
                    parsed.streams.forEach(s => clientSubscriptions.get(clientWs).streams.add(s));
                    console.log(`[Subscription] Client successfully subscribed to streams on ${targetNode.url}`);
                }
                if (isUnsubscribe && parsed.streams && clientSubscriptions.has(clientWs)) {
                    const sub = clientSubscriptions.get(clientWs);
                    parsed.streams.forEach(s => sub.streams.delete(s));
                    if (sub.streams.size === 0) clientSubscriptions.delete(clientWs);
                    console.log(`[Subscription] Client unsubscribed from streams on ${targetNode.url}`);
                }

                // 将结果原样转发回前端客户端
                if (clientWs.readyState === WebSocket.OPEN) {
                    clientWs.send(responseData.toString());
                }
            }
        });

        // 4. 经由常驻连接池发送给 XRPLD 节点
        try {
            targetNode.poolWs.send(message);
        } catch (err) {
            // 物理链路上发送失败的兜底清理
            const callbackInfo = responseCallbacks.get(reqId);
            if (callbackInfo) {
                responseCallbacks.delete(reqId);
                clientActiveRequests.delete(reqId);
                clearTimeout(callbackInfo.timeoutId);
                callbackInfo.callback(err);
            }
            triggerCircuitBreaker(targetNode, 'Offline');
        }
    });

    // ==========================================
    // 核心清理逻辑：客户端意外掉线 / 主动断开时触发
    // ==========================================
    clientWs.on('close', () => {
        frontendClientsCount--;
        console.log(`[Cleanup] Client disconnected. Starting resource cleanup...`);

        // 1. 清理该客户端尚未结束（仍在等待中）的请求回调，并【精准释放】对应后端的并发槽位
        for (const [reqId, associatedNode] of clientActiveRequests.entries()) {
            if (responseCallbacks.has(reqId)) {
                const callbackInfo = responseCallbacks.get(reqId);
                
                // 从全局多路复用映射表中抹除，防止后续后端返回时找不到客户端报错
                responseCallbacks.delete(reqId);
                // 取消对应的超时定时器，释放内存
                clearTimeout(callbackInfo.timeoutId);
                
                // 精准归还该请求占用的目标后端并发额度，防止死锁
                associatedNode.currentLoad = Math.max(0, associatedNode.currentLoad - 1);
                console.log(`[Cleanup] Released pending request callback and decremented load for Node: ${associatedNode.url}, Request ID: ${reqId}`);
            }
        }
        clientActiveRequests.clear();

        // 2. 清理订阅事件流，并向后端 XRPLD 发送优雅退订通知
        if (clientSubscriptions.has(clientWs)) {
            const subInfo = clientSubscriptions.get(clientWs);
            // 立即从本地映射中移除，切断向该客户端的流数据转发
            clientSubscriptions.delete(clientWs); 

            // 寻找此前承载该客户端订阅流的后端常驻长连接
            const targetNode = nodes.find(n => n.url === subInfo.nodeUrl);
            if (targetNode && targetNode.poolWs && targetNode.poolWs.readyState === WebSocket.OPEN) {
                // 代替已下线的客户端，向 XRPL 节点发送优雅退订指令，让其停止推送流数据
                const unsubscribeCmd = {
                    id: `proxy_auto_unsub_${Date.now()}_${Math.random()}`,
                    command: "unsubscribe",
                    streams: Array.from(subInfo.streams)
                };
                
                try {
                    targetNode.poolWs.send(JSON.stringify(unsubscribeCmd));
                    console.log(`[Cleanup] Sent 'unsubscribe' to ${targetNode.url} for streams: ${unsubscribeCmd.streams.join(', ')}`);
                } catch (e) {
                    console.error(`[Cleanup] Failed to send unsubscribe command to ${targetNode.url}`, e);
                }
            }
        }
    });
});

// ==========================================
// 8. 服务器监听启动
// ==========================================
server.listen(PORT, () => {
    console.log(`========================================================`);
    console.log(`🚀 XRPLD Multiplexing LB Proxy v2.0 Initialized`);
    console.log(`   - Running Port:       ${PORT}`);
    console.log(`   - Max Concurrency:    ${MAX_CONCURRENCY} per node`);
    console.log(`   - Request Timeout:    ${REQUEST_TIMEOUT}ms`);
    console.log(`   - Health Check:       Every ${HEALTH_CHECK_INTERVAL / 1000}s`);
    console.log(`========================================================`);
});

// 处理未捕获的异常，确保代理服务在极端边界情况下不崩溃
process.on('uncaughtException', (err) => {
    console.error(`[Fatal Error] Uncaught Exception:`, err);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Fatal Error] Unhandled Rejection at:`, promise, `reason:`, reason);
});
