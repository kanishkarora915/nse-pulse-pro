#!/usr/bin/env python3
"""
Quick Zerodha Token Generator
Run: python3 get_token.py
"""
import webbrowser, http.server, urllib.parse, hashlib, requests, json

API_KEY = "1fxr0x3bfgijtiqi"
API_SECRET = "aw11cy8faftpgsb4jr5kq1x9c9av60ce"
PORT = 9999

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)

        if "request_token" in params:
            rt = params["request_token"][0]
            print(f"\n✅ Got request_token: {rt}")

            # Exchange for access token
            checksum = hashlib.sha256(f"{API_KEY}{rt}{API_SECRET}".encode()).hexdigest()
            r = requests.post("https://api.kite.trade/session/token", data={
                "api_key": API_KEY,
                "request_token": rt,
                "checksum": checksum
            })
            data = r.json()

            if data.get("status") == "success":
                token = data["data"]["access_token"]
                print(f"✅ ACCESS TOKEN: {token}")
                print(f"\n👉 Ab site pe jao aur Direct Token mein ye daalo:")
                print(f"   API Key: {API_KEY}")
                print(f"   Access Token: {token}")

                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(f"""<html><body style="background:#0B0F1A;color:#EDF2F7;font-family:sans-serif;padding:40px;text-align:center">
                <h1 style="color:#34D399">✅ Token Generated!</h1>
                <p>Access Token:</p>
                <code style="background:#1a2236;padding:12px 20px;border-radius:8px;font-size:18px;color:#818CF8;display:inline-block;margin:10px">{token}</code>
                <br><br>
                <p>Ab <a href="https://nse-pulse-pro.onrender.com" style="color:#818CF8">NSE Pulse Pro</a> pe jao</p>
                <p><b>Direct Token</b> tab mein:</p>
                <p>API Key: <code style="color:#818CF8">{API_KEY}</code></p>
                <p>Access Token: <code style="color:#818CF8">{token}</code></p>
                </body></html>""".encode())
            else:
                print(f"❌ Error: {json.dumps(data, indent=2)}")
                self.send_response(400)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(f"<h2>Error</h2><pre>{json.dumps(data, indent=2)}</pre>".encode())
        else:
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h2>No request_token found</h2>")

        # Shutdown after handling
        import threading
        threading.Thread(target=self.server.shutdown).start()

    def log_message(self, *args): pass

print("=" * 50)
print("  Zerodha Token Generator")
print("=" * 50)
print(f"\n1. Browser khuleg Zerodha login ke liye...")
print(f"2. Login karo apne Zerodha credentials se")
print(f"3. Token automatically generate ho jayega!\n")

# Start local server to catch callback
server = http.server.HTTPServer(("127.0.0.1", PORT), Handler)

# Open Zerodha login - redirect to localhost
webbrowser.open(f"https://kite.zerodha.com/connect/login?v=3&api_key={API_KEY}")

print(f"Waiting for Zerodha callback on localhost:{PORT}...")
server.serve_forever()
print("\nDone! Server stopped.")
