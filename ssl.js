const http = require('http');
const tls = require('tls');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3001;
const CONFIG_PATH = path.join(__dirname, 'domains.json');

// 从配置文件中动态读取域名列表
function loadDomainList() {
    try {
        if (!fs.existsSync(CONFIG_PATH)) {
            const defaultList = ["example.com", "ripplefox.com:8443"];
            fs.writeFileSync(CONFIG_PATH, JSON.stringify(defaultList, null, 4), 'utf-8');
            return defaultList;
        }
        const content = fs.readFileSync(CONFIG_PATH, 'utf-8');
        return JSON.parse(content);
    } catch (error) {
        console.error('读取配置文件失败:', error.message);
        return [];
    }
}

// 核心改进：解析域名和端口，并抓取 SSL 证书信息
function getCertificateExpiry(domainInput) {
    return new Promise((resolve) => {
        let host = domainInput.trim();
        let port = 443; // 默认端口

        // 核心逻辑：判断是否显示指明了端口号 (例如 ripplefox.com:8443)
        if (host.includes(':')) {
            const parts = host.split(':');
            host = parts[0];
            port = parseInt(parts[1], 10) || 443;
        }

        const options = {
            host: host,
            port: port,
            servername: host, // 支持 SNI
            rejectUnauthorized: false // 即使证书过期也要允许连接以便抓取时间
        };

        const socket = tls.connect(options, () => {
            const cert = socket.getPeerCertificate();
            socket.end(); // 拿到证书立即断开连接

            if (!cert || !cert.valid_to) {
                return resolve({ display: domainInput, expiryDate: '未知', remainingDays: 9999, status: '未能抓取到证书' });
            }

            const expiryDate = new Date(cert.valid_to);
            const now = new Date();
            const remainingDays = Math.ceil((expiryDate - now) / (1000 * 60 * 60 * 24));

            resolve({
                display: `${host}:${port}`,
                expiryDate: expiryDate.toISOString().split('T')[0], // 格式化为 YYYY-MM-DD
                remainingDays,
                status: remainingDays <= 15 ? '⚠️ 证书即将过期' : '✅ 正常'
            });
        });

        // 错误处理（如网络不通、端口未开放等）
        socket.on('error', (err) => {
            resolve({ display: `${host}:${port}`, expiryDate: '无法连接', remainingDays: 9999, status: `连接失败: ${err.message}` });
        });

        // 5秒超时断开
        socket.setTimeout(5000, () => {
            socket.end();
            resolve({ display: `${host}:${port}`, expiryDate: '连接超时', remainingDays: 9999, status: '网络请求超时' });
        });
    });
}

// 创建 HTTP 服务
const server = http.createServer(async (req, res) => {
    if (req.url === '/favicon.ico') {
        res.writeHead(204);
        res.end();
        return;
    }

    const domainList = loadDomainList();

    if (domainList.length === 0) {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('❌ 监控清单为空，请检查 domains.json 配置文件。');
        return;
    }

    // 并发请求所有域名的证书信息
    const results = await Promise.all(domainList.map(domain => getCertificateExpiry(domain)));
    
    // 按证书剩余天数由近到远（升序）排序
    results.sort((a, b) => a.remainingDays - b.remainingDays);

    // 格式化纯文本表格
    let textOutput = `🌐 网站 HTTPS 证书到期监控清单 (支持自定义端口/已排序)\n`;
    textOutput += `==================================================================\n`;
    textOutput += `${'域名及端口'.padEnd(30)}${'证书到期日'.padEnd(15)}${'剩余天数'.padEnd(12)}状态\n`;
    textOutput += `==================================================================\n`;

    results.forEach(r => {
        const daysStr = r.remainingDays === 9999 ? 'N/A' : `${r.remainingDays} 天`;
        textOutput += `${r.display.padEnd(30)}${r.expiryDate.padEnd(15)}${daysStr.padEnd(12)}${r.status}\n`;
    });
    
    textOutput += `==================================================================\n`;
    textOutput += `更新时间: ${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}\n`;

    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(textOutput);
});

server.listen(PORT, () => {
    console.log(`证书监控服务已启动，正在监听端口: ${PORT}`);
});
