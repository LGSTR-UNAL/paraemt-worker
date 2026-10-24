"""Plugin contract: how a solver plugs into the worker.

A plugin implements three functions. The worker owns Kafka, S3 clients, status messages, timing,
provenance, the shared-artifact cache and cleanup; the plugin owns everything specific to the solver.

    prepare_shared(request, ctx)                  -> shared
        Build what every case of a campaign shares (line models, a steady-state snapshot) and return
        a handle (normally a directory path). The worker calls it once per `shared_key` and process,
        then reuses the handle for the next tasks with the same key.

    run_case(request, ctx, shared, case_dir)      -> RunInfo
        Create `case_dir` from `shared` plus the case's own inputs and run the solver there.
        Raise on failure; the worker publishes status=failed with the message.

    extract_results(request, ctx, case_dir)       -> list[Result]
        Reduce the solver output to (measurement, t, value) records. The worker publishes them to
        RESULTS_TOPIC. A plugin may instead store raw output (e.g. upload a file) and return [].
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Hashable, NamedTuple


class InvalidRequest(Exception):
    """The request names a plugin or carries values the worker cannot run."""


class Result(NamedTuple):
    measurement: str
    t: float
    value: float


@dataclass
class Context:
    """Services the worker lends to a plugin."""

    s3: Any
    producer: Any = None


@dataclass
class RunInfo:
    duration: float  # solver wall time in seconds
    details: dict[str, Any] = field(default_factory=dict)  # extra values for the metrics record


class Plugin(ABC):
    name: str
    version: str = "1"

    def workdir(self, request: dict[str, Any]) -> str:
        """Per-task working directory. The worker removes it after the task, failed or not."""
        return f"/tmp/{self.name}_{request['id']}"

    def shared_key(self, request: dict[str, Any]) -> Hashable:
        """Tasks with equal keys share one `prepare_shared` result."""
        return None

    @abstractmethod
    def prepare_shared(self, request: dict[str, Any], ctx: Context) -> Any: ...

    @abstractmethod
    def run_case(
        self, request: dict[str, Any], ctx: Context, shared: Any, case_dir: str
    ) -> RunInfo: ...

    @abstractmethod
    def extract_results(
        self, request: dict[str, Any], ctx: Context, case_dir: str
    ) -> list[Result]: ...

    def solver_info(self, ctx: Context) -> dict[str, Any]:
        """Solver identity for the provenance record."""
        return {"name": self.name, "version": None}

    def failure_artifacts(self, request: dict[str, Any], ctx: Context, case_dir: str) -> None:
        """Called after a failure, before cleanup (e.g. upload a solver listing)."""
