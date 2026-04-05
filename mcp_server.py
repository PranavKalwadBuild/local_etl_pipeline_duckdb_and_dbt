#!/usr/bin/env python3
import http.server
import json
import os
import socketserver

PORT = int(os.getenv("MCP_PORT", "8080"))


class MCPHandler(http.server.BaseHTTPRequestHandler):
    def _send_json(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ["/", "/health"]:
            self._send_json(200, {"status": "ok", "message": "MCP server is healthy."})
        else:
            self.send_error(404, "Not found")

    def do_POST(self):
        if self.path != "/mcp":
            self.send_error(404, "Not found")
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Invalid JSON payload"})
            return

        response = {
            "status": "ok",
            "received": payload,
            "result": {
                "message": "This is a minimal MCP server response.",
                "echo": payload.get("input"),
            },
        }
        self._send_json(200, response)

    def log_message(self, format, *args):
        return


def run_server(port: int = PORT):
    with socketserver.TCPServer(("", port), MCPHandler) as httpd:
        print(f"MCP server listening on http://localhost:{port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("Stopping MCP server...")
            httpd.server_close()


if __name__ == "__main__":
    run_server()
