import http.server
import socketserver
import argparse
from urllib.parse import urlparse
from rendezqueue.impl import RendezqueueImpl
from typing import cast, Tuple, Any


class RendezqueueServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    def __init__(
        self,
        server_address: Tuple[str, int],
        RequestHandlerClass: Any,
        impl: RendezqueueImpl,
        http_path: str = "/",
    ) -> None:
        self.impl = impl
        self.http_path = http_path
        super().__init__(server_address, RequestHandlerClass)


class RendezqueueHandler(http.server.BaseHTTPRequestHandler):
    def do_OPTIONS(self) -> None:
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

        parsed_path = urlparse(self.path)
        if parsed_path.path != server.http_path:
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
    parser.add_argument("--http_path", default="/", help="Path for RPC")
    parser.add_argument("--port_filepath", help="File to write the port number to")
    parser.add_argument("--ssl_cert", help="Path to SSL certificate")
    parser.add_argument("--ssl_key", help="Path to SSL key")

    args, unknown = parser.parse_known_args()

    impl = RendezqueueImpl()

    server = RendezqueueServer(
        (args.http_host, args.http_port),
        RendezqueueHandler,
        impl,
        http_path=args.http_path,
    )

    if args.ssl_cert:
        import ssl

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile=args.ssl_cert, keyfile=args.ssl_key)
        server.socket = context.wrap_socket(server.socket, server_side=True)

    host, port = server.server_address[:2]
    scheme = "https" if args.ssl_cert else "http"
    print(f"Server running at {scheme}://{host}:{port}{args.http_path}")

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
