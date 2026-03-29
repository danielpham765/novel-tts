#!/usr/bin/env python3
import selectors
import signal
import socket
import threading


LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 6379
TARGET_HOST = "on-premise"
TARGET_PORT = 6379
BUFFER_SIZE = 65536
BACKLOG = 128


def bridge(client: socket.socket, upstream: socket.socket) -> None:
    selector = selectors.DefaultSelector()
    selector.register(client, selectors.EVENT_READ, upstream)
    selector.register(upstream, selectors.EVENT_READ, client)
    sockets = (client, upstream)
    try:
        while True:
            events = selector.select()
            if not events:
                continue
            for key, _ in events:
                source = key.fileobj
                destination = key.data
                try:
                    chunk = source.recv(BUFFER_SIZE)
                except OSError:
                    return
                if not chunk:
                    return
                try:
                    destination.sendall(chunk)
                except OSError:
                    return
    finally:
        selector.close()
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass


def accept_loop() -> int:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((LISTEN_HOST, LISTEN_PORT))
    server.listen(BACKLOG)
    server.settimeout(1.0)

    should_stop = False

    def handle_stop(_signum: int, _frame: object) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    try:
        while not should_stop:
            try:
                client, _address = server.accept()
            except (TimeoutError, socket.timeout):
                continue
            except OSError:
                if should_stop:
                    break
                raise
            try:
                upstream = socket.create_connection((TARGET_HOST, TARGET_PORT))
            except OSError:
                client.close()
                continue
            worker = threading.Thread(
                target=bridge,
                args=(client, upstream),
                daemon=True,
            )
            worker.start()
    finally:
        server.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(accept_loop())
    except KeyboardInterrupt:
        raise SystemExit(0)
