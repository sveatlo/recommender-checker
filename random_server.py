#!/usr/bin/env python3
from http.server import HTTPServer, BaseHTTPRequestHandler


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'[12345]')

if __name__ == "__main__":
    httpd = HTTPServer(('localhost', 3000), SimpleHTTPRequestHandler)
    httpd.serve_forever()
