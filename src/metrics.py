from dataclasses import dataclass
from threading import Lock


@dataclass
class PipelineCounters:
    total_requests: int = 0
    total_files: int = 0
    total_failures: int = 0
    total_validation_failures: int = 0
    total_latency_ms: int = 0


class MetricsRegistry:
    def __init__(self) -> None:
        self._counters = PipelineCounters()
        self._lock = Lock()

    def record_request(self, file_count: int, latency_ms: int, failed: bool, validation_failed: int) -> None:
        with self._lock:
            self._counters.total_requests += 1
            self._counters.total_files += file_count
            self._counters.total_latency_ms += latency_ms
            if failed:
                self._counters.total_failures += 1
            self._counters.total_validation_failures += validation_failed

    def snapshot(self) -> dict[str, int | float]:
        with self._lock:
            avg_ms = (
                self._counters.total_latency_ms / self._counters.total_requests
                if self._counters.total_requests
                else 0.0
            )
            return {
                "total_requests": self._counters.total_requests,
                "total_files": self._counters.total_files,
                "total_failures": self._counters.total_failures,
                "total_validation_failures": self._counters.total_validation_failures,
                "avg_request_latency_ms": round(avg_ms, 2),
            }

