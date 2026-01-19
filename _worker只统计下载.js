import { connect } from 'cloudflare:sockets';

// ===========================================
// 默认配置
// ===========================================
const DEFAULT_UUID = '469cb497-03dd-4d8e-967d-366e0ffe9551';
const DEFAULT_SUBPATH = 'link';
const DEFAULT_PASSWORD = 'password';

// CDN 配置
const CFIP_LIST = [
  'nexusmods.com:443#♥  请勿滥用  ♥',
  'cloudflare.seeck.cn:443#♥Seeck三网通用线路♥',
  'ctcc.cloudflare.seeck.cn:443#♥Seeck电信专用线路♥',
  'cmcc.cloudflare.seeck.cn:443#♥Seeck移动专用线路♥',
  'cucc.cloudflare.seeck.cn:443#♥Seeck联通专用线路♥',
  'www.shopify.com:443#♥哄哄CDN线路 A♥',
  'www.ntu.edu.sg:443#♥哄哄CDN线路 B♥',
  'nexusmods.com:443#♥哄哄CDN线路 C♥',
  'www.cnae.top:443#♥哄哄CDN线路 D♥',
  'cdn.9889888.xyz:443#♥哄哄CDN线路 E♥',
  'yx.cloudflare.182682.xyz:443#♥哄哄CDN线路 F♥',
  'cloudflare.czkcdn.cn:443#♥哄哄CDN线路 G♥',
  'mfa.gov.ua:443#♥哄哄CDN线路 H♥',
  'saas.sin.fan:443#♥哄哄CDN线路 I♥',
  'cf.008500.xyz:443#♥哄哄CDN线路 J♥',
  'cf.877774.xyz:443#♥哄哄CDN线路 K♥',
  'cf.zhetengsha.eu.org:443#♥哄哄CDN线路 L♥',
  'sub.danfeng.eu.org:443#♥哄哄CDN线路 M♥',
  'cf.130519.xyz:443#♥哄哄CDN线路 N♥',
  'store.ubi.com:443#♥哄哄CDN线路 O♥',
  'cdns.doon.eu.org:443#♥哄哄CDN线路 P♥',
  'cf.090227.xyz:443#♥哄哄CDN线路 Q♥'
];

// 区域回源代理配置
const proxyIpAddrs = {
  EU: 'ProxyIP.DE.CMLiussss.net',
  HK: 'ProxyIP.HK.CMLiussss.net',
  AS: 'ProxyIP.SG.CMLiussss.net',
  JP: 'ProxyIP.JP.CMLiussss.net',
  US: 'ProxyIP.US.CMLiussss.net'
};
// ===========================================

const maxChunkLen = 16 * 1024;
const coloRegions = {
  JP: new Set(['FUK', 'ICN', 'KIX', 'NRT', 'OKA']),
  HK: new Set(['HKG', 'TPE', 'KHH']),
  EU: new Set([
    'ACC','ADB','ALA','ALG','AMM','AMS','ARN','ATH','BAH','BCN','BEG','BGW','BOD','BRU','BTS','BUD','CAI',
    'CDG','CPH','CPT','DAR','DKR','DMM','DOH','DUB','DUR','DUS','DXB','EBB','EDI','EVN','FCO','FRA','GOT',
    'GVA','HAM','HEL','HRE','IST','JED','JIB','JNB','KBP','KEF','KWI','LAD','LED','LHR','LIS','LOS','LUX',
    'LYS','MAD','MAN','MCT','MPM','MRS','MUC','MXP','NBO','OSL','OTP','PMO','PRG','RIX','RUH','RUN','SKG',
    'SOF','STR','TBS','TLL','TLV','TUN','VIE','VNO','WAW','ZAG','ZRH'
  ]),
  AS: new Set([
    'ADL','AKL','AMD','BKK','BLR','BNE','BOM','CBR','CCU','CEB','CGK','CMB','COK','DAC','DEL','HAN',
    'HYD','ISB','JHB','JOG','KCH','KHI','KTM','KUL','LHE','MAA','MEL','MFM','MLE','MNL','NAG','NOU',
    'PAT','PBH','PER','PNH','SGN','SIN','SYD','ULN','VTE'
  ])
};

const coloToProxyMap = new Map();
for (const [region, colos] of Object.entries(coloRegions)) {
  for (const colo of colos) coloToProxyMap.set(colo, proxyIpAddrs[region]);
}

const textDecoder = new TextDecoder();
const createConnect = (hostname, port, socket = connect({ hostname, port })) => socket.opened.then(() => socket);

/**
 * 管道传输，带流量回调
 */
const manualPipe = async (readable, writable, onTraffic) => {
  let chunkBuf = new ArrayBuffer(maxChunkLen);
  const reader = readable.getReader({ mode: 'byob' });
  try {
    while (true) {
      const { done, value } = await reader.read(new Uint8Array(chunkBuf));
      if (done) break;
      chunkBuf = value.buffer;
      if (onTraffic) onTraffic(value.byteLength);
      writable.send(value.slice());
    }
  } finally {
    reader.releaseLock();
  }
};

// =====================================================
// D1 总流量统计（每分钟 flush 一次）
// 绑定：env.DB（D1 Database）
// 表：traffic_counter(id TEXT PRIMARY KEY, bytes INTEGER)
// =====================================================
const FLUSH_INTERVAL_MS = 60_000;

// 注意：这是“同一个 isolate 内复用”的内存累计池。
// Cloudflare 可能回收 isolate，所以它不是“绝对不丢”的，但比每次断开都写 DB 更省写。
// 为了尽量准确：
// - 连接关闭时会把会话流量加到 pendingBytes
// - 只要到达 60 秒就 flush 一次
// - 首页访问也会触发一次 maybeFlush（避免长时间无 flush）
let pendingBytes = 0;
let lastFlushAt = Date.now();
let flushInFlight = null;

async function d1EnsureAndUpdate(env, addBytes) {
  // 直接 UPDATE（要求你先初始化插入 global=0）
  await env.DB.prepare(
    "UPDATE traffic_counter SET bytes = bytes + ? WHERE id = 'global'"
  ).bind(addBytes).run();
}

async function flushTraffic(env) {
  if (!env.DB) return; // 未绑定 D1 就直接跳过
  if (pendingBytes <= 0) return;

  // 防并发：同一时间只跑一个 flush
  if (flushInFlight) return flushInFlight;

  const bytesToWrite = pendingBytes;
  pendingBytes = 0;

  flushInFlight = (async () => {
    try {
      await d1EnsureAndUpdate(env, bytesToWrite);
    } catch (e) {
      // 写失败：把增量加回去，尽量不丢
      pendingBytes += bytesToWrite;
      console.error("D1 flush failed:", e);
    } finally {
      flushInFlight = null;
    }
  })();

  return flushInFlight;
}

async function addTrafficAndMaybeFlush(env, addBytes) {
  if (!addBytes || addBytes <= 0) return;
  pendingBytes += addBytes;

  const now = Date.now();
  if (now - lastFlushAt >= FLUSH_INTERVAL_MS) {
    lastFlushAt = now;
    await flushTraffic(env);
  }
}

async function maybeFlush(env) {
  const now = Date.now();
  if (pendingBytes > 0 && (now - lastFlushAt >= FLUSH_INTERVAL_MS)) {
    lastFlushAt = now;
    await flushTraffic(env);
  }
}

async function getTotalTrafficFromD1(env) {
  if (!env.DB) return 0;
  try {
    const row = await env.DB.prepare(
      "SELECT bytes FROM traffic_counter WHERE id='global' LIMIT 1"
    ).first();
    const n = row?.bytes;
    return typeof n === 'number' ? n : parseInt(n || "0") || 0;
  } catch (e) {
    console.error("D1 read error:", e);
    return 0;
  }
}

export default {
  async fetch(request, env, ctx) {
    const uuid = env.UUID || env.uuid || DEFAULT_UUID;
    const password = env.PASSWORD || env.password || DEFAULT_PASSWORD;
    let subPath = env.SUB_PATH || env.subpath || DEFAULT_SUBPATH;

    // 每个请求顺手检查一下是否需要 flush（避免长时间没有触发）
    ctx.waitUntil(maybeFlush(env));

    if (!subPath || subPath === 'link') subPath = uuid;

    const uuidBytes = new Uint8Array(16);
    const offsets = [0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 4, 4, 4, 4];
    for (let i = 0, c; i < 16; i++) {
      uuidBytes[i] =
        ((((c = uuid.charCodeAt(i * 2 + offsets[i])) > 64 ? c + 9 : c) & 0xF) << 4) |
        ((((c = uuid.charCodeAt(i * 2 + offsets[i] + 1)) > 64 ? c + 9 : c) & 0xF));
    }

    const handleWebSocketConn = async (webSocket, request) => {
      const protocolHeader = request.headers.get('sec-websocket-protocol');
      // @ts-ignore
      const earlyData = protocolHeader ? Uint8Array.fromBase64(protocolHeader, { alphabet: 'base64url' }) : null;
      let tcpWrite, processingChain = Promise.resolve(), tcpSocket;

      // 流量统计变量（按会话字节累计）
      let currentSessionTraffic = 0;

      const closeSocket = () => {
        if (currentSessionTraffic > 0) {
          // ✅ 不再每次断开就写数据库：改为累计 + 每分钟 flush
          ctx.waitUntil(addTrafficAndMaybeFlush(env, currentSessionTraffic));
        }
        if (!earlyData) { tcpSocket?.close(); webSocket?.close(); }
      };

      const processMessage = async (chunk) => {
        try {
// 只统计下载：不统计上行
// if (chunk) currentSessionTraffic += chunk.byteLength;

          if (tcpWrite) return tcpWrite(chunk);
          chunk = earlyData ? chunk : new Uint8Array(chunk);
          webSocket.send(new Uint8Array([chunk[0], 0]));

          for (let i = 0; i < 16; i++) if (chunk[i + 1] !== uuidBytes[i]) return null;

          let offset = 19 + chunk[17];
          const port = (chunk[offset] << 8) | chunk[offset + 1];
          offset += 2;
          const addrType = chunk[offset++];
          let newOffset, hostname;

          if (addrType === 2) {
            const len = chunk[offset++];
            newOffset = offset + len;
            hostname = textDecoder.decode(chunk.subarray(offset, newOffset));
          } else if (addrType === 1) {
            newOffset = offset + 4;
            const bytes = chunk.subarray(offset, newOffset);
            hostname = `${bytes[0]}.${bytes[1]}.${bytes[2]}.${bytes[3]}`;
          } else {
            newOffset = offset + 16;
            let ipv6Str = ((chunk[offset] << 8) | chunk[offset + 1]).toString(16);
            for (let i = 1; i < 8; i++) ipv6Str += ':' + ((chunk[offset + i * 2] << 8) | chunk[offset + i * 2 + 1]).toString(16);
            hostname = `[${ipv6Str}]`;
          }

          tcpSocket = await createConnect(hostname, port).catch(() => {
            const url = new URL(request.url);
            const proxyHost = url.searchParams.get('proxyip') ?? coloToProxyMap.get(request.cf?.colo) ?? proxyIpAddrs.US;
            return createConnect(proxyHost, 443);
          });

          const tcpWriter = tcpSocket.writable.getWriter();
          const payload = chunk.subarray(newOffset);
          if (payload.byteLength) tcpWriter.write(payload);
          tcpWrite = (chunk) => tcpWriter.write(chunk);

          // 下行流量
          manualPipe(tcpSocket.readable, webSocket, (len) => currentSessionTraffic += len);
        } catch {
          closeSocket();
        }
      };

      if (earlyData) processingChain = processingChain.then(() => processMessage(earlyData));
      webSocket.addEventListener('message', (event) => processingChain = processingChain.then(() => processMessage(event.data)));
      webSocket.addEventListener('close', () => closeSocket());
      webSocket.addEventListener('error', () => closeSocket());
    };

    const url = new URL(request.url);

    if (request.headers.get('Upgrade') === 'websocket') {
      const { 0: clientSocket, 1: webSocket } = new WebSocketPair();
      webSocket.accept();
      handleWebSocketConn(webSocket, request);
      return new Response(null, { status: 101, webSocket: clientSocket });
    }

    // 订阅（保持原逻辑）
    if (url.pathname.toLowerCase().includes(subPath.toLowerCase())) {
      const currentDomain = url.hostname;
      const vlsHeader = 'vless';

      // --- ADDAPI 功能处理 ---
      let finalCFIPList = [...CFIP_LIST];
      const addApiEnv = env.ADDAPI || env.addapi;

      if (addApiEnv) {
        const apiUrls = addApiEnv.split(',');
        const apiRequests = apiUrls.map(async (apiUrl) => {
          apiUrl = apiUrl.trim();
          if (!apiUrl) return;
          try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 2000);
            const res = await fetch(apiUrl, {
              headers: { 'User-Agent': 'Mozilla/5.0' },
              signal: controller.signal
            });
            clearTimeout(timeoutId);
            if (res.ok) {
              const text = await res.text();
              const lines = text.split(/\r?\n/)
                .map(line => line.trim())
                .filter(line => line && !line.startsWith('#'));
              if (lines.length > 0) finalCFIPList.push(...lines);
            }
          } catch (e) { }
        });
        await Promise.all(apiRequests);
      }

      const vlsLinks = finalCFIPList.map(cdnItem => {
        let host, port = 443, nodeName = '';
        if (cdnItem.includes('#')) {
          const parts = cdnItem.split('#');
          cdnItem = parts[0];
          nodeName = parts[1];
        }
        if (cdnItem.startsWith('[') && cdnItem.includes(']:')) {
          const ipv6End = cdnItem.indexOf(']:');
          host = cdnItem.substring(0, ipv6End + 1);
          const portStr = cdnItem.substring(ipv6End + 2);
          port = parseInt(portStr) || 443;
        } else if (cdnItem.includes(':')) {
          const parts = cdnItem.split(':');
          host = parts[0];
          port = parseInt(parts[1]) || 443;
        } else {
          host = cdnItem;
        }
        const vlsNodeName = nodeName ? `${nodeName}` : `Workers-${host}`;
        return `${vlsHeader}://${uuid}@${host}:${port}?encryption=none&security=tls&sni=${currentDomain}&fp=firefox&allowInsecure=1&type=ws&host=${currentDomain}&path=%2F#${encodeURIComponent(vlsNodeName)}`;
      }).join('\n');

      const base64Content = btoa(unescape(encodeURIComponent(vlsLinks)));
      return new Response(base64Content, {
        headers: {
          'Content-Type': 'text/plain; charset=utf-8',
          'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
        },
      });
    }

    // 主页
    if (request.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
      // 打开主页时也顺便 flush 一次（可让数据显示更接近实时）
      ctx.waitUntil(maybeFlush(env));
      return await getHomePage(request, env, password, subPath, uuid);
    }

    return new Response('Not Found', { status: 404 });
  }
};

/**
 * 主页获取逻辑
 */
async function getHomePage(request, env, password, subPath, uuid) {
  const host = request.headers.get('Host');
  const baseUrl = `https://${host}`;
  const urlObj = new URL(request.url);
  const providedPassword = urlObj.searchParams.get('password');

  // 从 D1 获取总流量
  const totalTraffic = await getTotalTrafficFromD1(env);

  if (providedPassword) {
    if (providedPassword === password) {
      return getMainPageContent(host, baseUrl, subPath, uuid, totalTraffic);
    } else {
      return getLoginPage(host, baseUrl, true);
    }
  }
  return getLoginPage(host, baseUrl, false);
}

// 登录页面保持不变
function getLoginPage(url, baseUrl, showError = false) {
  const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Workers Service - 登录</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #7dd3ca 0%, #a17ec4 100%); height: 100vh; display: flex; align-items: center; justify-content: center; color: #333; overflow: hidden; }
    .login-container { background: rgba(255, 255, 255, 0.95); backdrop-filter: blur(10px); border-radius: 20px; padding: 40px; box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1); max-width: 400px; width: 95%; text-align: center; }
    .title { font-size: 1.8rem; margin-bottom: 8px; color: #2d3748; }
    .subtitle { color: #718096; margin-bottom: 30px; font-size: 1rem; }
    .form-input { width: 100%; padding: 12px 16px; border: 2px solid #e2e8f0; border-radius: 8px; font-size: 1rem; margin-bottom: 20px; outline: none; transition: border-color 0.3s; }
    .form-input:focus { border-color: #667eea; }
    .btn-login { width: 100%; padding: 12px 20px; background: linear-gradient(135deg, #12cd9e 0%, #a881d0 100%); color: white; border: none; border-radius: 8px; font-size: 1rem; font-weight: 600; cursor: pointer; transition: all 0.3s ease; }
    .btn-login:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1); }
    .error-message { background: #fed7d7; color: #c53030; padding: 12px; border-radius: 8px; margin-bottom: 20px; font-size: 0.9rem; }
    .logo { font-size: 3rem; margin-bottom: 10px; }
  </style>
</head>
<body>
  <div class="login-container">
    <div class="logo">⚡</div>
    <h1 class="title">Workers Lite</h1>
    <p class="subtitle">请输入密码访问控制台</p>
    ${showError ? '<div class="error-message">密码错误,请重试</div>' : ''}
    <form onsubmit="handleLogin(event)">
      <input type="password" id="password" class="form-input" placeholder="请输入密码" required autofocus>
      <button type="submit" class="btn-login">登录</button>
    </form>
  </div>
  <script>
    function handleLogin(event) {
      event.preventDefault();
      const password = document.getElementById('password').value;
      const currentUrl = new URL(window.location);
      currentUrl.searchParams.set('password', password);
      window.location.href = currentUrl.toString();
    }
  </script>
</body>
</html>`;
  return new Response(html, { headers: { 'Content-Type': 'text/html;charset=utf-8' } });
}

// 主页内容（带单位换算）
function getMainPageContent(url, baseUrl, subPath, uuid, totalTraffic) {
  const formatBytes = (bytes) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formattedTraffic = formatBytes(totalTraffic);

  const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Workers Service</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #66ead7 0%, #9461c8 100%); height: 100vh; display: flex; align-items: center; justify-content: center; color: #333; }
    .container { background: rgba(255, 255, 255, 0.95); backdrop-filter: blur(10px); border-radius: 20px; padding: 30px; box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1); max-width: 800px; width: 95%; max-height: 90vh; overflow-y: auto; text-align: center; position: relative; }
    .logout-btn { position: absolute; top: 20px; right: 20px; background: #a7a0d8; color: #fff; border: none; border-radius: 8px; padding: 6px 12px; font-size: 0.9rem; cursor: pointer; }
    .title { font-size: 1.8rem; margin-bottom: 8px; color: #2d3748; }
    .subtitle { color: #718096; margin-bottom: 20px; }
    .info-card { background: #f7fafc; border-radius: 12px; padding: 15px; margin: 15px 0; border-left: 4px solid #6ed8c9; text-align: left; }
    .info-item { display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #e2e8f0; }
    .info-item:last-child { border-bottom: none; }
    .label { font-weight: 600; color: #4a5568; }
    .value { font-family: monospace; background: #edf2f7; padding: 4px 8px; border-radius: 6px; font-size: 0.9rem; word-break: break-all; color: #2d3748; }
    .value.traffic { color: #e53e3e; background: #fff5f5; font-weight: bold; }
    .btn { display: inline-block; padding: 10px 20px; background: linear-gradient(45deg, #667eea, #764ba2); color: white; border-radius: 8px; text-decoration: none; font-weight: 600; margin: 5px; border: none; cursor: pointer; transition: transform 0.2s; }
    .btn:hover { transform: translateY(-2px); }
    .toast { position: fixed; top: 20px; right: 20px; background: #48bb78; color: white; padding: 12px 20px; border-radius: 8px; opacity: 0; transition: opacity 0.3s; pointer-events: none; }
    .toast.show { opacity: 1; }
  </style>
</head>
<body>
  <div class="container">
    <button onclick="logout()" class="logout-btn">退出</button>
    <h1 class="title">Workers Lite</h1>
    <p class="subtitle">精简版 VLESS + D1 统计</p>

    <div class="info-card">
      <div class="info-item">
        <span class="label">已用总流量 (D1)</span>
        <span class="value traffic">${formattedTraffic}</span>
      </div>
      <div class="info-item">
        <span class="label">主机地址</span>
        <span class="value">${url}</span>
      </div>
      <div class="info-item">
        <span class="label">UUID</span>
        <span class="value">${uuid}</span>
      </div>
      <div class="info-item">
        <span class="label">完整订阅链接</span>
        <span class="value">${baseUrl}/${subPath}</span>
      </div>
    </div>

    <div style="margin-top: 20px;">
      <button onclick="copyLink('${baseUrl}/${subPath}')" class="btn">复制 V2rayN 订阅</button>
      <button onclick="copyLink('https://sublink.eooce.com/clash?config=${baseUrl}/${subPath}')" class="btn">复制 Clash 订阅</button>
    </div>
  </div>

  <div id="toast" class="toast">复制成功!</div>

  <script>
    function copyLink(text) {
      navigator.clipboard.writeText(text).then(() => {
        const toast = document.getElementById('toast');
        toast.classList.add('show');
        setTimeout(() => toast.classList.remove('show'), 2000);
      });
    }
    function logout() {
      const url = new URL(window.location);
      url.searchParams.delete('password');
      window.location.href = url.toString();
    }
  </script>
</body>
</html>`;
  return new Response(html, { headers: { 'Content-Type': 'text/html;charset=utf-8' } });
}
