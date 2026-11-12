import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .storage import Store


def _query(path):
    parts = urlsplit(path)
    return parts.path, {key: values[-1] for key, values in parse_qs(parts.query).items()}


def serve(database, port):
    Path(database).parent.mkdir(parents=True, exist_ok=True)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format_string, *arguments):
            return None

        def do_GET(self):
            route, query = _query(self.path)
            store = Store(database)
            try:
                if route == '/records':
                    status, value = 200, store.records()
                elif route == '/resources':
                    status, value = 200, store.resources(query.get('source'))
                elif route == '/health':
                    status, value = 200, store.source_health(query.get('source'))
                elif route == '/gaps':
                    status, value = 200, store.gaps(
                        status=query.get('status'),
                        source=query.get('source'),
                        epoch=query.get('epoch'),
                    )
                elif route == '/state':
                    status, value = 200, dict(
                        resources=store.resources(),
                        health=store.source_health(),
                        gaps=store.gaps(),
                    )
                else:
                    status = 404
                    value = {'error': '资源不存在'}
                data = json.dumps(value, ensure_ascii=False).encode('utf-8')
            finally:
                store.close()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    print(f'采集记录服务启动于 127.0.0.1:{server.server_port}', flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
