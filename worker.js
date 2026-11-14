import { connect } from "cloudflare:sockets";

const WS_PATH = "/home/go.html";
const PROXY_IP = "proxyip.cmliussss.net";
const CONNECT_TIMEOUT = 7000;
const FALLBACK_TIMEOUT = 10000;
const WINDOW = 4194304;
const BUDGET = 83886080;
const QUEUE_LIMIT = 4194304;
const FLUSH_SIZE = 32768;
const FLUSH_MS = 2;
const WRITE_BATCH_MAX = 65536;
const FAIL_TTL = 20000;
const FAIL_CACHE_MAX = 2000;

const RESPONSE_HEADER = new Uint8Array(2);
const HTML_HEADERS = new Headers({ "Content-Type": "text/html; charset=utf-8", Server: "nginx" });
const HTML_INIT = { headers: HTML_HEADERS };
const NOT_FOUND_INIT = { status: 404, headers: HTML_HEADERS };
const decoder = new TextDecoder();
const noop = () => {};
const TIMEOUT_ERR = new Error("Connect timeout");
const ABORT_ERR = new Error("Connect aborted");

const NGINX_BODY = new TextEncoder().encode(`<!DOCTYPE html>
<html>
<head>
<title>Welcome to nginx!</title>
<style>
html { color-scheme: light dark; }
body { width: 35em; margin: 0 auto;
font-family: Tahoma, Verdana, Arial, sans-serif; }
</style>
</head>
<body>
<h1>Welcome to nginx!</h1>
<p>If you see this page, the nginx web server is successfully installed and
working. Further configuration is required.</p>

<p>For online documentation and support please refer to
<a href="http://nginx.org/">nginx.org</a>.<br/>
Commercial support is available at
<a href="http://nginx.com/">nginx.com</a>.</p>

<p><em>Thank you for using nginx.</em></p>
</body>
</html>
`);
const NOT_FOUND_BODY = new TextEncoder().encode(`<html>
<head><title>404 Not Found</title></head>
<body>
<center><h1>404 Not Found</h1></center>
<hr><center>nginx</center>
</body>
</html>
`);

const B64 = new Int8Array(128).fill(-1);
const ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
for (let i = 0; i < 64; i++) B64[ALPHABET.charCodeAt(i)] = i;
B64[45] = 62;
B64[95] = 63;

const budgetWaiters = new Set();
const dialFailCache = new Map();
let total = 0;
let flushScratch = new Uint8Array(65536);

function wakeBudgetWaiters() {
  if (!budgetWaiters.size) return;
  for (const fn of budgetWaiters) fn();
  budgetWaiters.clear();
}

function markFailed(key) {
  if (dialFailCache.size >= FAIL_CACHE_MAX) {
    dialFailCache.delete(dialFailCache.keys().next().value);
  }
  dialFailCache.set(key, Date.now());
}

function parseProxyIp(qs) {
  if (!qs) return null;
  if (qs.startsWith("proxyip=") && !qs.includes("&") && !qs.includes("%") && !qs.includes("+")) {
    return qs.slice(8);
  }
  if (qs.includes("proxyip") || qs.includes("%")) {
    return new URLSearchParams(qs).get("proxyip");
  }
  return null;
}

function parseAck(str) {
  const len = str.length;
  if (!len || len > 16) return -1;
  let n = 0;
  for (let i = 0; i < len; i++) {
    const d = str.charCodeAt(i) - 48;
    if (d < 0 || d > 9) return -1;
    n = n * 10 + d;
  }
  return n <= Number.MAX_SAFE_INTEGER ? n : -1;
}

function upgradeWS(protocol, qs) {
  const { 0: client, 1: server } = new WebSocketPair();
  server.accept();
  server.binaryType = "arraybuffer";
  server.send(RESPONSE_HEADER);
  handleTraffic(server, qs, protocol ? decodeEarly(protocol) : null);
  return protocol
    ? new Response(null, { status: 101, webSocket: client, headers: { "Sec-WebSocket-Protocol": protocol } })
    : new Response(null, { status: 101, webSocket: client });
}

export default {
  fetch(req) {
    const raw = req.url;
    const schemeEnd = raw.indexOf("://") + 3;
    const pStart = raw.indexOf("/", schemeEnd);
    const qIdx = raw.indexOf("?", schemeEnd);
    const hIdx = raw.indexOf("#", schemeEnd);

    if (
      schemeEnd > 2 &&
      pStart !== -1 &&
      (qIdx === -1 || pStart < qIdx) &&
      (hIdx === -1 || pStart < hIdx)
    ) {
      const pEnd = qIdx !== -1 ? (hIdx !== -1 && hIdx < qIdx ? hIdx : qIdx) : (hIdx !== -1 ? hIdx : raw.length);
      const pLen = pEnd - pStart;

      if (pLen === 1) {
        return new Response(NGINX_BODY, HTML_INIT);
      }

      if (pLen === 13 && raw.startsWith(WS_PATH, pStart)) {
        const upg = req.headers.get("Upgrade");
        if (upg && (upg === "websocket" || upg.toLowerCase() === "websocket")) {
          const qs =
            qIdx !== -1 && (hIdx === -1 || qIdx < hIdx)
              ? raw.slice(qIdx + 1, hIdx === -1 ? raw.length : hIdx)
              : null;
          return upgradeWS(req.headers.get("Sec-WebSocket-Protocol"), qs);
        }
        return new Response(NOT_FOUND_BODY, NOT_FOUND_INIT);
      }

      const dotIdx = raw.indexOf("/.", pStart);
      const slsIdx = raw.indexOf("\\", schemeEnd);
      const pctIdx = raw.indexOf("%2", pStart);
      if (
        (dotIdx === -1 || dotIdx >= pEnd) &&
        (slsIdx === -1 || slsIdx >= pEnd) &&
        (pctIdx === -1 || pctIdx >= pEnd)
      ) {
        return new Response(NOT_FOUND_BODY, NOT_FOUND_INIT);
      }
    }

    const url = new URL(raw);
    const path = url.pathname;
    if (path === WS_PATH) {
      const upg = req.headers.get("Upgrade");
      if (upg && (upg === "websocket" || upg.toLowerCase() === "websocket")) {
        return upgradeWS(req.headers.get("Sec-WebSocket-Protocol"), url.search ? url.search.slice(1) : null);
      }
    }
    return path === "/"
      ? new Response(NGINX_BODY, HTML_INIT)
      : new Response(NOT_FOUND_BODY, NOT_FOUND_INIT);
  }
};

function decodeEarly(value) {
  let n = value.length;
  if (n < 24) return null;
  if ((n & 3) === 0) {
    if (value.charCodeAt(n - 1) === 61) n--;
    if (value.charCodeAt(n - 1) === 61) n--;
  }
  if ((n & 3) === 1) return null;
  const size = (n * 3) >> 2;
  if (size < 18) return null;
  const out = new Uint8Array(size);
  const end = n - (n & 3);
  let i = 0;
  let o = 0;
  while (i < end) {
    const c1 = value.charCodeAt(i);
    const c2 = value.charCodeAt(i + 1);
    const c3 = value.charCodeAt(i + 2);
    const c4 = value.charCodeAt(i + 3);
    if ((c1 | c2 | c3 | c4) > 127) return null;
    const a = B64[c1];
    const b = B64[c2];
    const c = B64[c3];
    const d = B64[c4];
    if ((a | b | c | d) < 0) return null;
    const v = (a << 18) | (b << 12) | (c << 6) | d;
    out[o] = v >> 16;
    out[o + 1] = v >> 8;
    out[o + 2] = v;
    i += 4;
    o += 3;
  }
  const rem = n - end;
  if (rem === 2) {
    const c1 = value.charCodeAt(i);
    const c2 = value.charCodeAt(i + 1);
    if ((c1 | c2) > 127) return null;
    const a = B64[c1];
    const b = B64[c2];
    if ((a | b) < 0) return null;
    out[o] = (a << 2) | (b >> 4);
  } else if (rem === 3) {
    const c1 = value.charCodeAt(i);
    const c2 = value.charCodeAt(i + 1);
    const c3 = value.charCodeAt(i + 2);
    if ((c1 | c2 | c3) > 127) return null;
    const a = B64[c1];
    const b = B64[c2];
    const c = B64[c3];
    if ((a | b | c) < 0) return null;
    out[o] = (a << 2) | (b >> 4);
    out[o + 1] = (b << 4) | (c >> 2);
  }
  return out;
}

function dial(hostname, port, ms, setCancel) {
  let socket;
  try {
    socket = connect({ hostname, port });
  } catch (err) {
    return Promise.reject(err);
  }
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        setCancel(null);
        socket.close().catch(noop);
        reject(TIMEOUT_ERR);
      }
    }, ms);
    setCancel(() => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        socket.close().catch(noop);
        reject(ABORT_ERR);
      }
    });
    socket.opened.then(
      () => {
        if (!settled) {
          settled = true;
          setCancel(null);
          clearTimeout(timer);
          resolve(socket);
        }
      },
      (err) => {
        if (!settled) {
          settled = true;
          setCancel(null);
          clearTimeout(timer);
          socket.close().catch(noop);
          reject(err);
        }
      }
    );
  });
}

function handleTraffic(ws, qs, early) {
  let tcp = null;
  let writer = null;
  let cancelDial = null;
  let started = early !== null;
  let closed = false;
  let sent = 0;
  let acked = 0;
  let credit = false;
  let acct = 0;
  let queued = 0;
  let writing = false;
  const writeQueue = [];
  let writeHead = 0;
  const buf = [];
  let bufCount = 0;
  let len = 0;
  let timer = 0;
  let resume = null;

  const setDialCancel = (fn) => {
    cancelDial = fn;
  };

  const hold = (r) => {
    resume = r;
  };

  const wake = () => {
    const r = resume;
    if (r) {
      resume = null;
      r();
    }
  };

  const flush = () => {
    if (timer) {
      clearTimeout(timer);
      timer = 0;
    }
    const count = bufCount;
    const n = len;
    if (!n || closed || ws.readyState !== 1) {
      for (let i = 0; i < count; i++) buf[i] = null;
      bufCount = 0;
      len = 0;
      return;
    }
    let out;
    if (count === 1) {
      out = buf[0];
      buf[0] = null;
    } else {
      if (flushScratch.length < n) flushScratch = new Uint8Array(n);
      let o = 0;
      for (let i = 0; i < count; i++) {
        const c = buf[i];
        buf[i] = null;
        flushScratch.set(c, o);
        o += c.byteLength;
      }
      out = n === flushScratch.length ? flushScratch : flushScratch.subarray(0, n);
    }
    if (buf.length > 64) buf.length = 0;
    bufCount = 0;
    len = 0;
    try {
      ws.send(out);
    } catch {
      close();
      return;
    }
    sent += n;
    if (credit) {
      acct += n;
      total += n;
    }
  };

  const close = () => {
    if (closed) {
      if (tcp) {
        const s = tcp;
        tcp = null;
        s.close().catch(noop);
      }
      return;
    }
    closed = true;
    budgetWaiters.delete(wake);
    if (cancelDial) {
      const fn = cancelDial;
      cancelDial = null;
      fn();
    }
    if (timer) {
      clearTimeout(timer);
      timer = 0;
    }
    buf.length = 0;
    bufCount = 0;
    len = 0;
    writeQueue.length = 0;
    writeHead = 0;
    queued = 0;
    credit = false;
    if (acct) {
      total -= acct;
      if (total < 0) total = 0;
      acct = 0;
      wakeBudgetWaiters();
    }
    wake();
    if (tcp) {
      const s = tcp;
      tcp = null;
      s.close().catch(noop);
    }
    if (!writing && writer) {
      try {
        writer.releaseLock();
      } catch {}
      writer = null;
    }
    try {
      if (ws.readyState < 2) ws.close();
    } catch {}
  };

  const runWriterPump = async () => {
    writing = true;
    try {
      while (!closed && writeHead < writeQueue.length) {
        const ds = writer.desiredSize;
        if (ds === null) {
          close();
          break;
        }
        if (ds <= 0) {
          await writer.ready;
          if (closed) break;
        }
        const available = writeQueue.length - writeHead;
        let out;
        let outLen;
        if (available === 1) {
          out = writeQueue[writeHead];
          writeQueue[writeHead++] = null;
          outLen = out.byteLength;
        } else {
          let batchLen = 0;
          let end = writeHead;
          while (end < writeQueue.length) {
            const nextLen = writeQueue[end].byteLength;
            if (batchLen > 0 && batchLen + nextLen > WRITE_BATCH_MAX) break;
            batchLen += nextLen;
            end++;
          }
          if (end - writeHead === 1) {
            out = writeQueue[writeHead];
            writeQueue[writeHead++] = null;
            outLen = out.byteLength;
          } else {
            out = new Uint8Array(batchLen);
            let off = 0;
            while (writeHead < end) {
              const c = writeQueue[writeHead];
              writeQueue[writeHead++] = null;
              out.set(c, off);
              off += c.byteLength;
            }
            outLen = batchLen;
          }
        }
        if (writeHead === writeQueue.length) {
          writeQueue.length = 0;
          writeHead = 0;
        } else if (writeHead >= 64) {
          writeQueue.splice(0, writeHead);
          writeHead = 0;
        }
        await writer.write(out);
        if (closed) break;
        queued -= outLen;
      }
    } catch {
      close();
    } finally {
      writing = false;
      if (closed && writer) {
        try {
          writer.releaseLock();
        } catch {}
        writer = null;
      }
    }
  };

  const start = async (data) => {
    const dLen = data.byteLength;
    if (
      dLen < 23 ||
      data[1] !== 0xd3 || data[2] !== 0x42 || data[3] !== 0xd1 || data[4] !== 0x1e ||
      data[5] !== 0xd4 || data[6] !== 0x24 || data[7] !== 0x45 || data[8] !== 0x83 ||
      data[9] !== 0xb3 || data[10] !== 0x6e || data[11] !== 0x52 || data[12] !== 0x4a ||
      data[13] !== 0xb1 || data[14] !== 0xf0 || data[15] !== 0xaf || data[16] !== 0xa4
    ) {
      return close();
    }

    const pIdx = 19 + data[17];
    const offset0 = pIdx + 3;
    if (offset0 >= dLen) return close();

    const port = (data[pIdx] << 8) | data[pIdx + 1];
    if (!port) return close();
    const type = data[pIdx + 2];
    let host;
    let offset;

    if (type === 1) {
      offset = offset0 + 4;
      if (offset > dLen) return close();
      host = `${data[offset0]}.${data[offset0 + 1]}.${data[offset0 + 2]}.${data[offset0 + 3]}`;
    } else if (type === 2) {
      const addrLen = data[offset0];
      const addrStart = offset0 + 1;
      offset = addrStart + addrLen;
      if (offset > dLen) return close();
      host = addrLen ? decoder.decode(data.subarray(addrStart, offset)) : "";
    } else if (type === 3) {
      offset = offset0 + 16;
      if (offset > dLen) return close();
      host =
        ((data[offset0] << 8) | data[offset0 + 1]).toString(16) + ":" +
        ((data[offset0 + 2] << 8) | data[offset0 + 3]).toString(16) + ":" +
        ((data[offset0 + 4] << 8) | data[offset0 + 5]).toString(16) + ":" +
        ((data[offset0 + 6] << 8) | data[offset0 + 7]).toString(16) + ":" +
        ((data[offset0 + 8] << 8) | data[offset0 + 9]).toString(16) + ":" +
        ((data[offset0 + 10] << 8) | data[offset0 + 11]).toString(16) + ":" +
        ((data[offset0 + 12] << 8) | data[offset0 + 13]).toString(16) + ":" +
        ((data[offset0 + 14] << 8) | data[offset0 + 15]).toString(16);
    } else {
      return close();
    }

    if (offset < dLen) {
      const initLen = dLen - offset;
      if (queued + initLen > QUEUE_LIMIT) return close();
      queued += initLen;
      writeQueue.push(data.subarray(offset));
    }

    let tcpHost = host;
    let tcpPort = port;
    const dialKey = tcpHost + ":" + tcpPort;
    const failedAt = dialFailCache.get(dialKey);
    let direct = !failedAt || Date.now() - failedAt > FAIL_TTL;

    if (direct) {
      try {
        tcp = await dial(tcpHost, tcpPort, CONNECT_TIMEOUT, setDialCancel);
        if (failedAt) dialFailCache.delete(dialKey);
      } catch {
        markFailed(dialKey);
        direct = false;
      }
    }

    if (!direct) {
      if (closed || ws.readyState !== 1) return close();
      const customProxy = parseProxyIp(qs);
      if (customProxy) {
        const colon = customProxy.indexOf(":");
        if (colon === -1) {
          tcpHost = customProxy;
        } else {
          tcpHost = customProxy.slice(0, colon);
          const nextColon = customProxy.indexOf(":", colon + 1);
          const pStr = nextColon === -1 ? customProxy.slice(colon + 1) : customProxy.slice(colon + 1, nextColon);
          if (pStr) tcpPort = +pStr;
        }
      } else {
        tcpHost = PROXY_IP;
      }
      try {
        tcp = await dial(tcpHost, tcpPort, FALLBACK_TIMEOUT, setDialCancel);
      } catch {
        return close();
      }
    }

    if (closed || ws.readyState !== 1) return close();

    writer = tcp.writable.getWriter();
    if (!writing && writeHead < writeQueue.length) runWriterPump();

    const reader = tcp.readable.getReader();
    try {
      while (!closed && ws.readyState === 1) {
        if (credit && (sent + len - acked > WINDOW || total > BUDGET)) {
          if (len) flush();
          while (!closed && ws.readyState === 1 && credit && (sent - acked > WINDOW || total > BUDGET)) {
            if (total > BUDGET) budgetWaiters.add(wake);
            await new Promise(hold);
            budgetWaiters.delete(wake);
          }
          if (closed || ws.readyState !== 1) break;
        }
        let readRes;
        try {
          readRes = await reader.read();
        } catch {
          break;
        }
        if (readRes.done || closed || ws.readyState !== 1) break;
        const value = readRes.value;
        const vLen = value.byteLength;
        if (!vLen) continue;
        if (!len && vLen >= FLUSH_SIZE) {
          try {
            ws.send(value);
          } catch {
            break;
          }
          sent += vLen;
          if (credit) {
            acct += vLen;
            total += vLen;
          }
        } else {
          buf[bufCount++] = value;
          len += vLen;
          if (len >= FLUSH_SIZE) flush();
          else if (!timer) timer = setTimeout(flush, FLUSH_MS);
        }
      }
      if (len) flush();
    } finally {
      try {
        reader.releaseLock();
      } catch {}
    }
    close();
  };

  ws.addEventListener("message", (ev) => {
    if (closed) return;
    const data = ev.data;
    if (typeof data === "string") {
      const n = parseAck(data);
      if (n < 0 || n > sent) return;
      if (!credit) {
        credit = true;
        if (n > acked) acked = n;
        const d = sent - acked - acct;
        if (d) {
          acct += d;
          total += d;
          if (d < 0) wakeBudgetWaiters();
        }
        wake();
      } else if (n > acked) {
        const dec = n - acked;
        acked = n;
        acct -= dec;
        total -= dec;
        wakeBudgetWaiters();
        wake();
      }
      return;
    }

    const chunk =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : data instanceof Uint8Array
          ? data
          : ArrayBuffer.isView(data)
            ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
            : null;
    if (!chunk) return close();

    if (started) {
      if (queued + chunk.byteLength > QUEUE_LIMIT) return close();
      queued += chunk.byteLength;
      writeQueue.push(chunk);
      if (writer && !writing) runWriterPump();
    } else {
      started = true;
      start(chunk).catch(close);
    }
  });

  ws.addEventListener("close", close);
  ws.addEventListener("error", close);

  if (early) start(early).catch(close);
}
