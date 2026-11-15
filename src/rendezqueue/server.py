import http.server
import socketserver
import argparse
from urllib.parse import urlparse
from rendezqueue.impl import RendezqueueImpl
from rendezqueue.plyswap_wire import ROUTE
from rendezqueue.plyswap_cache import DEFAULT_TTL, PlySwapCache
from rendezqueue.plyswap_http import handle as handle_plyswap
from typing import cast, Tuple, Any, Optional


class RendezqueueServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    def __init__(
        self,
        server_address: Tuple[str, int],
        RequestHandlerClass: Any,
        impl: RendezqueueImpl,
        http_path: str = ROUTE,
        tryswap_http_path: Optional[str] = None,
        plyswap_ttl: float = DEFAULT_TTL,
    ) -> None:
        paths = (
            (http_path,)
            if tryswap_http_path is None
            else (http_path, tryswap_http_path)
        )
        for path in paths:
            if (
                not path.startswith("/")
                or path.startswith("//")
                or any(char in path for char in "?#")
                or any(char.isspace() for char in path)
            ):
                raise ValueError("endpoint must be a full absolute HTTP path")
        if tryswap_http_path == http_path:
            raise ValueError("TrySwap and PlySwap paths must differ")
        self.impl = impl
        self.tryswap_http_path = tryswap_http_path
        self.http_path = http_path
        self.plyswap = PlySwapCache(plyswap_ttl)
        super().__init__(server_address, RequestHandlerClass)

    def service_actions(self) -> None:
        # serve_forever's maintenance runs independently of request body I/O.
        self.plyswap.sweep()


class RendezqueueHandler(http.server.BaseHTTPRequestHandler):
    def _is_plyswap(self) -> bool:
        path = self.path.partition("?")[0]
        return path == cast(RendezqueueServer, self.server).http_path

    def _is_tryswap(self) -> bool:
        path = cast(RendezqueueServer, self.server).tryswap_http_path
        return path is not None and urlparse(self.path).path == path

    def send_error(self, code: int, message: Any = None, explain: Any = None) -> None:
        # BaseHTTPRequestHandler routes unsupported verbs through 501 here.
        # PlySwap uses 405, including OPTIONS.
        if code == 501 and self._is_plyswap():
            server = cast(RendezqueueServer, self.server)
            handle_plyswap(self, server.plyswap, server.http_path)
            return
        super().send_error(code, message, explain)

    def do_OPTIONS(self) -> None:
        if self._is_plyswap():
            server = cast(RendezqueueServer, self.server)
            handle_plyswap(self, server.plyswap, server.http_path)
            return
        if not self._is_tryswap():
            self.send_error(404, "Not Found")
            return
        self.send_response(204)
        if "Access-Control-Request-Headers" in self.headers:
            self.send_header(
                "Access-Control-Allow-Headers",
                self.headers["Access-Control-Request-Headers"],
            )
        if "Access-Control-Request-Method" in self.headers:
            self.send_header(
                "Access-Control-Allow-Methods",
                self.headers["Access-Control-Request-Method"],
            )
        if "Origin" in self.headers:
            self.send_header("Access-Control-Allow-Origin", self.headers["Origin"])
        self.end_headers()

    def do_POST(self) -> None:
        server = cast(RendezqueueServer, self.server)

        if self._is_plyswap():
            handle_plyswap(self, server.plyswap, server.http_path)
            return

        if not self._is_tryswap():
            self.send_error(404, "Not Found")
            return

        content_type = self.headers.get("Content-Type", "")
        if content_type != "application/json":
            self.send_error(418, "I'm a teapot")
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")

        result = server.impl.tryswap_string(body)

        if isinstance(result, int):
            self.send_response(result)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Type", "application/json")
            self.end_headers()
        else:
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(result.encode("utf-8"))


def run() -> None:
    parser = argparse.ArgumentParser(description="Rendezqueue Server")
    parser.add_argument("--http_host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument(
        "--http_port", type=int, default=0, help="Port to bind to (0 for random)"
    )
    parser.add_argument(
        "--tryswap_http_path",
        help="Enable TrySwap at this full path",
    )
    parser.add_argument("--http_path", default=ROUTE, help="Full path for PlySwap RPC")
    parser.add_argument(
        "--plyswap_ttl",
        type=float,
        default=DEFAULT_TTL,
        help="PlySwap inactivity TTL in seconds",
    )
    parser.add_argument("--port_filepath", help="File to write the port number to")
    parser.add_argument("--ssl_cert", help="Path to SSL certificate")
    parser.add_argument("--ssl_key", help="Path to SSL key")

    args = parser.parse_args()

    impl = RendezqueueImpl()

    server = RendezqueueServer(
        (args.http_host, args.http_port),
        RendezqueueHandler,
        impl,
        tryswap_http_path=args.tryswap_http_path,
        http_path=args.http_path,
        plyswap_ttl=args.plyswap_ttl,
    )

    if args.ssl_cert:
        import ssl

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile=args.ssl_cert, keyfile=args.ssl_key)
        server.socket = context.wrap_socket(server.socket, server_side=True)

    host, port = server.server_address[:2]
    scheme = "https" if args.ssl_cert else "http"
    print(f"PlySwap at {scheme}://{host}:{port}{args.http_path}")
    if args.tryswap_http_path is not None:
        print(f"TrySwap at {scheme}://{host}:{port}{args.tryswap_http_path}")

    if args.port_filepath:
        with open(args.port_filepath, "w") as f:
            f.write(str(port))

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run()
