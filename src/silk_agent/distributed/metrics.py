"""Small Prometheus-text metrics registry with bounded label cardinality.

Conversation keys, users, and text are never labels; callers pass short enumerations
(result, kind). A series beyond the cap is dropped rather than growing without bound.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Final

_MAX_SERIES: Final = 500
_MAX_LABEL_VALUE: Final = 40
LATENCY_BUCKETS: Final = (0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900)

Labels = tuple[tuple[str, str], ...]


class Metrics:
    def __init__(self, namespace: str = "silk_agent") -> None:
        self._namespace = namespace
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, Labels], float] = {}
        self._gauges: dict[tuple[str, Labels], float] = {}
        self._histograms: dict[tuple[str, Labels], list[float]] = {}
        self._help: dict[str, str] = {}
        self._collectors: list[Callable[[Metrics], None]] = []

    def describe(self, name: str, text: str) -> None:
        self._help[name] = text

    def add_collector(self, collector: Callable[[Metrics], None]) -> None:
        """Run at scrape time to refresh gauges from live process state."""
        self._collectors.append(collector)

    def inc(self, name: str, value: float = 1, **labels: str) -> None:
        self._update(self._counters, name, labels, lambda current: current + value, 0.0)

    def set(self, name: str, value: float, **labels: str) -> None:
        self._update(self._gauges, name, labels, lambda _current: value, 0.0)

    def observe(self, name: str, seconds: float, **labels: str) -> None:
        key = (name, _labels(labels))
        with self._lock:
            series = self._histograms.get(key)
            if series is None:
                if self._series_count() >= _MAX_SERIES:
                    return
                series = [0.0] * (len(LATENCY_BUCKETS) + 2)  # buckets, +Inf, sum
                self._histograms[key] = series
            for index, bound in enumerate(LATENCY_BUCKETS):
                if seconds <= bound:
                    series[index] += 1
            series[len(LATENCY_BUCKETS)] += 1
            series[len(LATENCY_BUCKETS) + 1] += max(0.0, seconds)

    def _update(self, store, name, labels, update, default) -> None:
        key = (name, _labels(labels))
        with self._lock:
            if key not in store and self._series_count() >= _MAX_SERIES:
                return
            store[key] = update(store.get(key, default))

    def _series_count(self) -> int:
        return len(self._counters) + len(self._gauges) + len(self._histograms)

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()

    def value(self, name: str, **labels: str) -> float | None:
        key = (name, _labels(labels))
        with self._lock:
            if key in self._counters:
                return self._counters[key]
            return self._gauges.get(key)

    def render(self) -> str:
        for collector in self._collectors:
            try:
                collector(self)
            except Exception:
                continue  # one broken collector must not hide the other series
        lines: list[str] = []
        with self._lock:
            self._emit(lines, "counter", self._counters)
            self._emit(lines, "gauge", self._gauges)
            for name in sorted({name for name, _ in self._histograms}):
                full = f"{self._namespace}_{name}"
                lines.append(f"# HELP {full} {self._help.get(name, name)}")
                lines.append(f"# TYPE {full} histogram")
                for (series_name, labels), series in sorted(self._histograms.items()):
                    if series_name != name:
                        continue
                    for index, bound in enumerate(LATENCY_BUCKETS):
                        lines.append(
                            f"{full}_bucket{_render(labels, le=str(bound))} {series[index]:g}"
                        )
                    count = series[len(LATENCY_BUCKETS)]
                    lines.append(f"{full}_bucket{_render(labels, le='+Inf')} {count:g}")
                    lines.append(f"{full}_count{_render(labels)} {count:g}")
                    lines.append(f"{full}_sum{_render(labels)} {series[-1]:.6f}")
        return "\n".join(lines) + "\n"

    def _emit(self, lines: list[str], kind: str, store: dict[tuple[str, Labels], float]) -> None:
        for name in sorted({name for name, _ in store}):
            full = f"{self._namespace}_{name}"
            lines.append(f"# HELP {full} {self._help.get(name, name)}")
            lines.append(f"# TYPE {full} {kind}")
            for (series_name, labels), value in sorted(store.items()):
                if series_name == name:
                    lines.append(f"{full}{_render(labels)} {value:g}")


def _labels(labels: dict[str, str]) -> Labels:
    return tuple(sorted((key, str(value)[:_MAX_LABEL_VALUE]) for key, value in labels.items()))


def _render(labels: Labels, **extra: str) -> str:
    items = [*labels, *extra.items()]
    if not items:
        return ""
    escaped = ",".join(
        f'{key}="{value.replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"'
        for key, value in items
    )
    return "{" + escaped + "}"


METRICS = Metrics()
