"""Unblock asyncio when Proton VPN forbids 127.0.0.1.

Windows asyncio builds its self-pipe with socket.socketpair(), which uses TCP
loopback. Proton WireGuard returns WinError 10013 or hangs on 127.0.0.1, so
asyncio.run() never enters your coroutine. Pair the sockets on the process's
non-loopback IPv4 instead (VPN 10.2.0.2 or LAN). Import this module before
asyncio.run / Playwright.
"""
from __future__ import annotations

import socket

_orig_socketpair = socket.socketpair
_patched = False


def _outbound_ipv4() -> str:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.settimeout(1)
        probe.connect(("1.1.1.1", 80))
        ip = probe.getsockname()[0]
        if ip and not ip.startswith("127."):
            return ip
    except OSError:
        pass
    finally:
        probe.close()
    raise OSError("no non-loopback IPv4 for asyncio socketpair")


def _lan_socketpair(family=None, type=socket.SOCK_STREAM, proto=0):
    if type != socket.SOCK_STREAM or proto != 0:
        return _orig_socketpair(family, type, proto)
    if family not in (None, socket.AF_INET, getattr(socket, "AF_INET6", None)):
        return _orig_socketpair(family, type, proto)

    host = _outbound_ipv4()
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind((host, 0))
        lsock.listen(1)
        port = lsock.getsockname()[1]
        csock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            csock.settimeout(3.0)
            csock.connect((host, port))
            ssock, _ = lsock.accept()
        except BaseException:
            csock.close()
            raise
        csock.settimeout(None)
        ssock.settimeout(None)
        return ssock, csock
    finally:
        lsock.close()


def patch() -> None:
    global _patched
    if _patched:
        return
    socket.socketpair = _lan_socketpair  # type: ignore[method-assign]
    _patched = True


patch()
