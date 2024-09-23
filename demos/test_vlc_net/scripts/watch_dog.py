# sudo apt install python3-pip
# pip install watchdog --break-system-packages
# python3 watch_dog.py /path/to/your/logfile.log --port 8000

import os
import http.server
import socketserver
import argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

parser = argparse.ArgumentParser(description='Simple HTTP server to view log files.')
parser.add_argument('log_file_path', type=str, help='Path to the log file to be monitored')
parser.add_argument('--port', type=int, default=8000, help='Port to serve HTTP requests (default: 8000)')
args = parser.parse_args()

LOG_FILE_PATH = args.log_file_path
PORT = args.port

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, file_path):
        self.file_path = file_path
        self._content = self._read_file()

    def _read_file(self):
        try:
            with open(self.file_path, 'r') as file:
                return file.read()
        except (FileNotFoundError, PermissionError) as e:
            print(f"Error reading log file: {e}")
            return ""

    def on_modified(self, event):
        if event.src_path == self.file_path:
            self._content = self._read_file()

    @property
    def content(self):
        return self._content

log_handler = LogFileHandler(LOG_FILE_PATH)
observer = Observer()
observer.schedule(log_handler, os.path.dirname(LOG_FILE_PATH), recursive=False)
observer.start()

class CustomHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/logs':
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(log_handler.content.encode())
        else:
            self.send_response(404)
            self.end_headers()

with socketserver.TCPServer(("", PORT), CustomHandler) as httpd:
    print(f"Serving at port {PORT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("Server stopped by user")
    finally:
        observer.stop()
        observer.join()
