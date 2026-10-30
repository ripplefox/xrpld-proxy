# XRPLD 负载均衡代理服务文档

这是一个基于 Node.js 开发的 **XRPLD (XRP Ledger) 负载均衡代理服务器**。它通过 WebSocket 协议接收客户端请求，按轮询算法分发至后端节点，并提供并发控制与动态健康检查功能。
系统单端口（默认3000）同时复用 HTTP 监控看板与 WebSocket 协议。

---

## 核心特性

* **轮询分发 (Round-Robin)**：请求按顺序均匀分发到各可用后端服务器。
* **并发限制 (Concurrency Control)**：限制每台后端服务器的单次最高并发数为 **3**。超出限制时立即拦截并向客户端返回 `busy` 错误。
* **动态健康检查 (Health Check)**：每 30 秒自动发送 `server_state` 命令，仅当服务器状态为 `full` 或 `validating` 时才允许接收流量。

---

## 系统架构与流程

节点列表： `wss://xrplcluster.com`, `wss://s1.ripple.com`, `wss://s2.ripple.com`, `wss://honeycluster.io`

## 网页监控看板
通过访问根目录或`/status`查看纯文本监控。输出内容如下：
```text
========================================================
 XRPLD LB CLUSTER STATUS
========================================================
 Active Frontend Clients: 1

 [Xrpld Cluster]
  - wss://xrplcluster.com     [● Online]    Load: [   ] 0/3
  - wss://xrpl.ws             [● Online]    Load: [   ] 0/3
  - wss://xrpl.link           [● Online]    Load: [   ] 0/3
========================================================
```
*   `[● Online  ]`：物理网络及业务层均稳定。
*   `[⚠️ Isolated]`：已被熔断保护（频繁超时、高频触顶或服务器未同步）。
*   `[○ Offline ]`：TCP 链路直接断开。
