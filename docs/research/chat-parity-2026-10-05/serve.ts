import { realpath } from "node:fs/promises";
import { resolve, sep } from "node:path";

// Keep the local report and its evidence files private to this machine.
const root = await realpath(import.meta.dir);
const port = Number(process.env.REPORT_PORT ?? 4782);
const server = Bun.serve({
  hostname: "127.0.0.1",
  port,
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("Method not allowed", { status: 405 });
    }
    let path: string;
    try {
      const relative = decodeURIComponent(url.pathname).replace(/^\/+/, "") || "index.html";
      path = await realpath(resolve(root, relative));
    } catch {
      return new Response("Not found", { status: 404 });
    }
    if (!path.startsWith(root + sep) || !/\.(html|css|js|json|md)$/.test(path)) {
      return new Response("Not found", { status: 404 });
    }
    const file = Bun.file(path);
    return new Response(request.method === "HEAD" ? null : file, {
      headers: {
        "Content-Type": file.type || "text/plain; charset=utf-8",
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
      },
    });
  },
});
console.log(`Chat capability report: ${server.url}`);
