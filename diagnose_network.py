"""Check only the configured public source host/port; keep TLS verification on."""
import argparse
import json
import socket
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path

HOST, PORT = "air.cnemc.cn", 18007


def diagnose():
    result = {"checked_utc": datetime.now(timezone.utc).isoformat(), "host": HOST, "port": PORT, "addresses": []}
    try:
        addresses = socket.getaddrinfo(HOST, PORT, type=socket.SOCK_STREAM)
    except OSError as error:
        result["dns_error"] = str(error)
        return result
    seen = set()
    # At most two unique resolved endpoints, eight seconds per connection stage.
    for family, kind, protocol, _, address in addresses:
        if address[0] in seen:
            continue
        seen.add(address[0])
        probe = {"address": address[0], "family": "IPv6" if family == socket.AF_INET6 else "IPv4"}
        stage = "tcp"
        start = time.monotonic()
        try:
            with socket.socket(family, kind, protocol) as sock:
                sock.settimeout(8)
                sock.connect(address)
                probe["tcp"] = "ok"
                stage = "tls"
                with ssl.create_default_context().wrap_socket(sock, server_hostname=HOST) as secure:
                    probe["tls"] = secure.version()
                    stage = "http"
                    secure.sendall(f"GET / HTTP/1.1\r\nHost: {HOST}:{PORT}\r\nConnection: close\r\n\r\n".encode())
                    probe["http_status"] = secure.recv(256).split(b"\r\n", 1)[0].decode("ascii", errors="replace")
        except (OSError, ssl.SSLError) as error:
            probe["failed_stage"] = stage
            probe["error"] = str(error)
        probe["elapsed_seconds"] = round(time.monotonic() - start, 2)
        result["addresses"].append(probe)
        if len(seen) == 2:
            break
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    text = json.dumps(diagnose(), ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    print(text)
