import random
from collections import deque
from datetime import timedelta

TagDict = dict[str, str | int | bool]


class StatsClientBase:
    """A Base class for various statsd clients."""

    def __init__(self, prefix=None) -> None:
        self._prefix = prefix
        self.global_tags: TagDict = {}

    def close(self):
        """Used to close and clean up any underlying resources."""
        raise NotImplementedError()

    def _send(self, data):
        raise NotImplementedError()

    def pipeline(self) -> "PipelineBase":
        raise NotImplementedError()

    def timer(self, stat, rate=1, tags: TagDict | None = None):
        from .timer import Timer

        return Timer(self, stat, rate, tags)

    def timing(self, stat, delta, rate=1, tags: TagDict | None = None):
        """
        Send new timing information.

        `delta` can be either a number of milliseconds or a timedelta.
        """
        if isinstance(delta, timedelta):
            # Convert timedelta to number of milliseconds.
            delta = delta.total_seconds() * 1000.0
        self._send_stat(stat, "%0.6f|ms" % delta, rate, tags)

    def incr(self, stat, count=1, rate=1, tags: TagDict | None = None):
        """Increment a stat by `count`."""
        self._send_stat(stat, "%s|c" % count, rate, tags)

    def decr(self, stat, count=1, rate=1, tags: TagDict | None = None):
        """Decrement a stat by `count`."""
        self.incr(stat, -count, rate, tags)

    def gauge(self, stat, value, rate=1, delta=False, tags: TagDict | None = None):
        """Set a gauge value."""
        if value < 0 and not delta:
            if rate < 1 and random.random() > rate:
                return
            with self.pipeline() as pipe:
                pipe._send_stat(stat, "0|g", 1, tags)
                pipe._send_stat(stat, "%s|g" % value, 1, tags)
        else:
            prefix = "+" if delta and value >= 0 else ""
            self._send_stat(stat, f"{prefix}{value}|g", rate, tags)

    def set(self, stat, value, rate=1, tags: TagDict | None = None):
        """Set a set value."""
        self._send_stat(stat, "%s|s" % value, rate, tags)

    def _send_stat(self, stat, value, rate, tags: TagDict | None):
        self._after(self._prepare(stat, value, rate, tags))

    def _prepare(self, stat, value, rate, tags: TagDict | None):
        if rate < 1:
            if random.random() > rate:
                return
            value = f"{value}|@{rate}"

        if self._prefix:
            stat = f"{self._prefix}.{stat}"

        all_tags = {**self.global_tags, **(tags or {})}

        if len(all_tags) > 0:
            tagstr = ",".join([f"{key}:{value}" for key, value in all_tags.items()])
            return f"{stat}:{value}|{tagstr}"

        return f"{stat}:{value}"

    def _after(self, data):
        if data:
            self._send(data)


class PipelineBase(StatsClientBase):
    def __init__(self, client: StatsClientBase):
        self._client = client
        self._prefix = client._prefix
        self._stats = deque()

    def _send_pipeline(self):
        raise NotImplementedError()

    def _after(self, data):
        if data is not None:
            self._stats.append(data)

    def __enter__(self):
        return self

    def __exit__(self, typ, value, tb):
        self.send()

    def send(self):
        if not self._stats:
            return
        self._send_pipeline()

    def pipeline(self):
        return self.__class__(self)
