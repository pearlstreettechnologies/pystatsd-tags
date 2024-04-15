import logging
import socket

from .base import PipelineBase, StatsClientBase

logger = logging.getLogger(__name__)


class Pipeline(PipelineBase):
    def __init__(self, client: "StatsClient"):
        super().__init__(client)
        self._maxudpsize = client._maxudpsize

    def _send_pipeline(self):
        data = self._stats.popleft()
        while self._stats:
            # Use popleft to preserve the order of the stats.
            stat = self._stats.popleft()
            if len(stat) + len(data) + 1 >= self._maxudpsize:
                self._client._after(data)
                data = stat
            else:
                data += "\n" + stat
        self._client._after(data)


class StatsClient(StatsClientBase):
    """A udp client for statsd."""

    def __init__(self, host="localhost", port=8125, prefix=None, maxudpsize=512, ipv6=False):
        """Create a new client."""
        super().__init__(prefix)
        fam = socket.AF_INET6 if ipv6 else socket.AF_INET
        family, _, _, _, addr = socket.getaddrinfo(host, port, fam, socket.SOCK_DGRAM)[0]
        self._addr = addr
        self._sock: socket.socket | None = socket.socket(family, socket.SOCK_DGRAM)
        self._maxudpsize = maxudpsize

    def _send(self, data):
        """Send data to statsd."""
        try:
            assert self._sock, "Socket not set up"
            self._sock.sendto(data.encode("ascii"), self._addr)
        except (OSError, RuntimeError):
            logger.debug(data)
            pass

    def close(self):
        if self._sock and hasattr(self._sock, "close"):
            self._sock.close()
        self._sock = None

    def pipeline(self):
        return Pipeline(self)
