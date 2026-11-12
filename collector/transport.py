import base64
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class Client:
    def __init__(self, timeout=5):
        self.timeout = timeout

    def request(self, url, method='GET', headers=None, body=None):
        request = Request(url, data=body, headers=headers or {}, method=method)
        try:
            response = urlopen(request, timeout=self.timeout)
        except HTTPError as error:
            response = error
        with response:
            return response.status, dict(response.headers), response.read()


def repair_path(source, epoch, start, end):
    """Path used by the catalog query client.

    Kept as a separate function so a demo/test server can script exactly the
    same path that the client will request.
    """

    query = urlencode(dict(source=source, epoch=epoch, start=int(start), end=int(end)))
    return f'/repair?{query}'


class CatalogClient:
    """Minimal catalog range-query client.

    The catalog endpoint returns one response object containing ``range``,
    ``complete``, ``terminal`` and ``items``.  A transport or JSON failure is
    surfaced to the caller as a failed fetch; the store records it as a failed
    repair attempt and keeps the gap open.
    """

    def __init__(self, base_url, timeout=5, client=None):
        self.base_url = base_url.rstrip('/')
        self.client = client or Client(timeout=timeout)

    def fetch(self, source, epoch, start, end):
        url = self.base_url + repair_path(source, epoch, start, end)
        status, headers, raw = self.client.request(url)
        try:
            payload = json.loads(raw.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            payload = dict(error=f'目录响应不是 JSON: {error}')
        return status, url, payload


@contextmanager
def replay_server(routes):
    positions = {}
    calls = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format_string, *arguments):
            return None

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            length = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(length)
            with lock:
                calls.append(dict(method=self.command, path=self.path, headers=dict(self.headers), body=body.decode('utf-8', errors='replace')))
                replies = routes.get(self.path, [dict(status=404, body='未找到资源')])
                index = positions.get(self.path, 0)
                reply = replies[min(index, len(replies) - 1)]
                positions[self.path] = index + 1
            data = base64.b64decode(reply['base64']) if 'base64' in reply else reply.get('body', '').encode('utf-8')
            self.send_response(reply.get('status', 200))
            for name, value in reply.get('headers', {}).items():
                self.send_header(name, value)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
