import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  // Disable body parsing to keep the request as a raw stream
  api: { bodyParser: false },

  // Enable streaming responses
  supportsResponseStreaming: true,

  // Maximum execution time (seconds)
  maxDuration: 60,
};

// Read target domain from env and remove trailing slash if present
const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

// Headers that should NOT be forwarded (hop-by-hop or proxy-sensitive)
const STRIP_HEADERS = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

export default async function handler(req, res) {
  // Ensure target domain is configured
  if (!TARGET_BASE) {
    res.statusCode = 500;
    return res.end("Misconfigured: TARGET_DOMAIN is not set");
  }

  try {
    // Build full upstream URL (preserve path and query)
    const targetUrl = TARGET_BASE + req.url;

    const headers = {};
    let clientIp = null;

    // Normalize and filter incoming headers
    for (const key of Object.keys(req.headers)) {
      const k = key.toLowerCase();
      const v = req.headers[key];

      // Skip hop-by-hop headers
      if (STRIP_HEADERS.has(k)) continue;

      // Skip Vercel internal headers
      if (k.startsWith("x-vercel-")) continue;

      // Capture client IP for forwarding
      if (k === "x-real-ip") { clientIp = v; continue; }
      if (k === "x-forwarded-for") { if (!clientIp) clientIp = v; continue; }

      // Flatten array headers into comma-separated string
      headers[k] = Array.isArray(v) ? v.join(", ") : v;
    }

    // Re-attach client IP if available
    if (clientIp) headers["x-forwarded-for"] = clientIp;

    const method = req.method;

    // Only non-GET/HEAD requests can have a body
    const hasBody = method !== "GET" && method !== "HEAD";

    // Prepare fetch options for upstream request
    const fetchOpts = { method, headers, redirect: "manual" };

    if (hasBody) {
      // Convert Node stream to Web stream for fetch
      fetchOpts.body = Readable.toWeb(req);

      // Required for streaming request bodies in Node fetch
      fetchOpts.duplex = "half";
    }

    // Send request to upstream server
    const upstream = await fetch(targetUrl, fetchOpts);

    // Forward status code
    res.statusCode = upstream.status;

    // Forward response headers
    for (const [k, v] of upstream.headers) {
      // Do not manually set transfer-encoding
      if (k.toLowerCase() === "transfer-encoding") continue;

      try { res.setHeader(k, v); } catch {}
    }

    if (upstream.body) {
      // Stream upstream response directly to client
      await pipeline(Readable.fromWeb(upstream.body), res);
    } else {
      // End response if no body
      res.end();
    }
  } catch (err) {
    // Log error for debugging
    console.error("relay error:", err);

    // Return proxy error if response not already started
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway: Tunnel Failed");
    }
  }
}
