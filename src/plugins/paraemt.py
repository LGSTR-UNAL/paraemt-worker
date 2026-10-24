"""ParaEMT plugin: electromagnetic-transient simulation of generation trips and governor steps.

    prepare_shared   steady-state snapshot of the system, built once per (system, ParaEMT commit):
                     taken from S3 if another worker already built it, built (~35 s for IEEE 39)
                     and uploaded otherwise
    run_case         per-case copy of ParaEMT's main_step1_simulation.py with the constants replaced,
                     run from the snapshot in an isolated working directory
    extract_results  summary of the saved bus quantities: frequency nadir, RoCoF, voltage extremes,
                     and the minimum voltage of every bus

ParaEMT is driven by editing constants in its script, so it is not modified: this plugin writes a
per-case copy. The public release has generation trips and governor/exciter reference steps; it
has no fault model. See docs/paraemt.md for the request fields.
"""

import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from importlib import metadata
from pathlib import Path
from typing import Any, Hashable

from .base import Context, InvalidRequest, Plugin, Result, RunInfo

SYSTEMS = {"2gen": 1, "ieee9": 2, "ieee39": 3, "wecc179": 4, "wecc240": 5, "2area": 6}
TS = 50e-6  # ParaEMT time step, seconds; part of the snapshot file name
SNAPSHOT_TLEN = 1.0  # seconds of steady state simulated to build the snapshot
DEFAULTS: dict[str, Any] = dict(
    system="ieee9", gen_index=0, t_trip=0.2, t_end=1.0, ds_rate=10,
    dsp=None, t_step=None, reinit=None, gen_step=None,
)
SNAPSHOT_BUCKET = os.getenv("S3_RESOURCES_BUCKET", "hydraflowsim-resources")
LOG_BUCKET = os.getenv("S3_RESULTS_BUCKET", "hydraflowsim-results")

# Scalar metrics published as results: name -> key of the time to attach (None = 0.0).
SCALAR_METRICS = {
    "vtm_min_pu": None,
    "vtm_max_pu": None,
    "freq_dev_min": None,
    "freq_dev_max": None,
    "freq_final_mean": None,
    "f_nadir_mean": "t_nadir",
    "rocof_max_pu_s": None,
}


def paraemt_home() -> Path:
    return Path(os.getenv("PARAEMT_HOME", "/opt/paraemt/ParaEMT_public")).resolve()


def paraemt_python() -> str:
    return os.getenv("PARAEMT_PYTHON") or sys.executable


def paraemt_commit() -> str:
    return os.getenv("PARAEMT_COMMIT") or "unversioned"


def _timeout() -> float:
    return float(os.getenv("PARAEMT_TIMEOUT_S", "3600"))


def prepare_case(request: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalise the case parameters of a request (pure)."""
    p = dict(DEFAULTS)
    p.update({k: request[k] for k in DEFAULTS if k in request})
    if p["system"] not in SYSTEMS:
        raise InvalidRequest(f"Unknown ParaEMT system: {p['system']!r}")
    try:
        p["t_end"] = float(p["t_end"])
        p["ds_rate"] = int(p["ds_rate"])
        p["gen_index"] = int(p["gen_index"])
        if p["t_trip"] is not None:
            p["t_trip"] = float(p["t_trip"])
        if p["dsp"] is not None:
            p["dsp"] = float(p["dsp"])
            if p["t_step"] is None:
                raise InvalidRequest("dsp (governor step) needs t_step")
            p["t_step"] = float(p["t_step"])
        if p["gen_step"] is not None:
            p["gen_step"] = int(p["gen_step"])
    except (TypeError, ValueError) as e:
        raise InvalidRequest(f"Invalid ParaEMT parameter: {e}") from e
    if p["t_trip"] is not None and p["t_trip"] >= p["t_end"]:
        raise InvalidRequest("t_trip must be < t_end")
    if p["dsp"] is not None and p["t_step"] >= p["t_end"]:
        raise InvalidRequest("t_step must be < t_end")
    if p["t_trip"] is None and p["dsp"] is None:
        raise InvalidRequest("The case needs a disturbance: t_trip or dsp")
    return p


def _set(src: str, name: str, value: Any) -> str:
    """Replace the first `name = ...` assignment (keeping the trailing comment)."""
    pat = re.compile(rf"^(\s*{re.escape(name)}\s*=\s*)([^#\n]*?)(\s*(#.*)?)$", re.M)
    if not pat.search(src):
        raise KeyError(f"constant not found in ParaEMT script: {name}")
    return pat.sub(lambda m: f"{m.group(1)}{value!r}{m.group(3)}", src, count=1)


def _base_script(system: int, tlen: float, simmod: int, dsrate: int) -> str:
    s = (paraemt_home() / "main_step1_simulation.py").read_text(encoding="utf8")
    for k, v in (("SimMod", simmod), ("DSrate", dsrate), ("systemN", system), ("Tlen", tlen), ("ts", TS)):
        s = _set(s, k, v)
    return s


def _snapshot_name(system: str) -> str:
    return f"sim_snp_S{SYSTEMS[system]}_{int(TS * 1e6)}u_1pt.pkl"


def _env() -> dict[str, str]:
    # One core per worker pod: keep numba/BLAS single-threaded.
    return dict(
        os.environ,
        PYTHONPATH=str(paraemt_home()),
        PYTHONIOENCODING="utf8",
        NUMBA_CACHE_DIR=os.getenv("NUMBA_CACHE_DIR", "/tmp/numba_cache"),
        NUMBA_NUM_THREADS="1",
        OMP_NUM_THREADS="1",
        MKL_NUM_THREADS="1",
        OPENBLAS_NUM_THREADS="1",
    )


def _run_script(script: str, workdir: Path) -> float:
    """Run a ParaEMT script in `workdir`; return its wall time. Raises with the output tail."""
    (workdir / "sim.py").write_text(script, encoding="utf8")
    t0 = time.time()
    try:
        p = subprocess.run(
            [paraemt_python(), "sim.py"], cwd=workdir, env=_env(),
            capture_output=True, text=True, timeout=_timeout(),
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"ParaEMT exceeded PARAEMT_TIMEOUT_S={_timeout():.0f} s") from e
    wall = time.time() - t0
    (workdir / "stdout.txt").write_text(p.stdout + p.stderr, encoding="utf8")
    if p.returncode != 0:
        raise RuntimeError(f"ParaEMT failed (rc={p.returncode}): {(p.stdout + p.stderr)[-600:]}")
    return wall


def _copy_inputs(workdir: Path) -> None:
    """ParaEMT reads cases/ and models/ relative to its working directory."""
    for d in ("cases", "models"):
        if not (workdir / d).exists():
            shutil.copytree(paraemt_home() / d, workdir / d)


def _link_or_copy(src: Path, dst: Path) -> None:
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def _snapshot_key(system: str) -> str:
    return f"paraemt/snapshots/{paraemt_commit()}/{system}_t{SNAPSHOT_TLEN}/{_snapshot_name(system)}"


class ParaEmtPlugin(Plugin):
    name = "paraemt"
    version = "1"

    def workdir(self, request: dict[str, Any]) -> str:
        return f"/tmp/paraemt_{request['id']}"

    def shared_key(self, request: dict[str, Any]) -> Hashable:
        # The snapshot depends only on the system (and the ParaEMT commit, fixed per image).
        return prepare_case(request)["system"]

    def prepare_shared(self, request: dict[str, Any], ctx: Context) -> str:
        system = prepare_case(request)["system"]
        base = Path(os.getenv("PARAEMT_CACHE_DIR", "/tmp/_paraemt_cache"))
        shared = base / f"{system}_{paraemt_commit()}"
        info_file = shared / "shared.json"
        if info_file.exists():
            return str(shared)
        shared.mkdir(parents=True, exist_ok=True)
        _copy_inputs(shared)
        snap = shared / _snapshot_name(system)
        source, build_s = None, None
        s3 = getattr(ctx, "s3", None)
        if s3 is not None:
            try:
                s3.download_file(SNAPSHOT_BUCKET, _snapshot_key(system), str(snap))
                source = "s3"
            except Exception:
                logging.info("paraemt: no snapshot in S3 for %s, building it", system)
        if source is None:
            build_s = self._build_snapshot(system, shared)
            source = "built"
            if s3 is not None:
                try:
                    s3.upload_file(str(snap), SNAPSHOT_BUCKET, _snapshot_key(system))
                except Exception:
                    logging.warning("paraemt: could not upload the snapshot to S3", exc_info=True)
        info_file.write_text(json.dumps({"snapshot_source": source, "snapshot_build_s": build_s}))
        return str(shared)

    @staticmethod
    def _build_snapshot(system: str, shared: Path) -> float:
        """Steady state with no disturbance; the 1-point snapshot is shared by all cases."""
        s = _base_script(SYSTEMS[system], SNAPSHOT_TLEN, simmod=0, dsrate=10)
        s = _set(s, "emt.flag_gentrip", 1)   # 1 = trip pending (never fires); 0 would mean "already tripped"
        s = _set(s, "emt.flag_reinit", 0)    # free-running steady state (re-init every step would freeze it)
        s = _set(s, "emt.flag_sc", 0)
        s = _set(s, "emt.t_gentrip", 1000)
        s = _set(s, "emt.t_sc", 100)
        wall = _run_script(s, shared)
        if not (shared / _snapshot_name(system)).exists():
            raise RuntimeError("ParaEMT did not write the steady-state snapshot")
        return wall

    def run_case(
        self, request: dict[str, Any], ctx: Context, shared: str, case_dir: str
    ) -> RunInfo:
        p = prepare_case(request)
        n = SYSTEMS[p["system"]]
        wd = Path(case_dir)
        wd.mkdir(parents=True, exist_ok=True)
        _copy_inputs(wd)
        name = _snapshot_name(p["system"])
        _link_or_copy(Path(shared) / name, wd / name)

        s = _base_script(n, p["t_end"], simmod=1, dsrate=p["ds_rate"])
        if p["t_trip"] is not None:
            s = _set(s, "emt.t_gentrip", p["t_trip"])
            s = _set(s, "emt.i_gentrip", p["gen_index"])
            s = _set(s, "emt.flag_gentrip", 1)
        else:
            s = _set(s, "emt.t_gentrip", 1000)
            s = _set(s, "emt.flag_gentrip", 1)
        if p["dsp"] is not None:   # governor/exciter reference step instead of (or with) a trip
            s = _set(s, "emt.t_sc", int(round(p["t_step"] / TS)))
            s = _set(s, "emt.dsp", p["dsp"])
            gen_step = p["gen_step"] if p["gen_step"] is not None else p["gen_index"]
            s = _set(s, "emt.i_gen_sc", gen_step)
            s = _set(s, "emt.flag_sc", 1)
        else:
            s = _set(s, "emt.flag_sc", 0)
        # ParaEMT re-initialises after a trip; a no-disturbance control must not.
        reinit = p["reinit"] if p["reinit"] is not None else (1 if p["t_trip"] is not None else 0)
        s = _set(s, "emt.flag_reinit", reinit)

        wall = _run_script(s, wd)
        details: dict[str, Any] = {}
        info_file = Path(shared) / "shared.json"
        if info_file.exists():
            details.update(json.loads(info_file.read_text()))
        return RunInfo(duration=wall, details=details)

    def extract_results(
        self, request: dict[str, Any], ctx: Context, case_dir: str
    ) -> list[Result]:
        p = prepare_case(request)
        summary = _summarise(Path(case_dir), p)
        return summary_to_results(summary)

    def solver_info(self, ctx: Context) -> dict[str, Any]:
        def version(package: str) -> str | None:
            try:
                return metadata.version(package)
            except metadata.PackageNotFoundError:
                return None

        return {
            "name": "paraemt",
            "version": os.getenv("PARAEMT_COMMIT") or None,
            "python": platform.python_version(),
            "numpy": version("numpy"),
            "numba": version("numba"),
            "scipy": version("scipy"),
        }

    def failure_artifacts(self, request: dict[str, Any], ctx: Context, case_dir: str) -> None:
        """Upload the ParaEMT output so the failure can be diagnosed."""
        out = Path(case_dir) / "stdout.txt"
        if not out.exists() or ctx.s3 is None:
            return
        body = out.read_bytes()[-200_000:]
        ctx.s3.put_object(
            Bucket=LOG_BUCKET,
            Key=f"paraemt/{request.get('run_id', 'norun')}/{request['id']}.log",
            Body=body,
        )


_EXTRACT = r'''
import json, pickle, sys
import numpy as np
with open(sys.argv[1], "rb") as f:
    pfd, dyd, ini, emt = pickle.load(f)
nb = len(pfd.bus_num)
xb = np.asarray(emt.x_bus)           # (6*nb, nsave): per bus ze,de,we,vt,vtm,dvtm
t = np.asarray(emt.t); t0 = float(sys.argv[2])
m = t > t0 + 0.01                    # post-disturbance window; skip the switching sample at the trip instant
vtm = xb[4::6][:, m]; we = xb[2::6][:, m]
out = {"n_saved": int(xb.shape[1]), "t_end": float(emt.t[-1]),
       "vtm_min_pu": float(vtm.min()), "vtm_max_pu": float(vtm.max()),
       "bus_num": [int(x) for x in pfd.bus_num],
       "bus_vtm_min": [float(x) for x in vtm.min(axis=1)],
       "freq_dev_min": float(we.min() - 1.0), "freq_dev_max": float(we.max() - 1.0),
       "freq_final_mean": float(we[:, -1].mean()), "n_bus": nb}
tw = t[m]; fm = we.mean(axis=0)                  # mean bus frequency (pu of nominal)
out["t_nadir"] = float(tw[np.argmin(fm)]); out["f_nadir_mean"] = float(fm.min() - 1.0)
w = (tw >= tw[0]) & (tw <= tw[0] + 0.5)
out["rocof_max_pu_s"] = float(np.abs(np.gradient(fm[w], tw[w])).max()) if w.sum() > 3 else None
print(json.dumps(out))
'''


def _summarise(workdir: Path, p: dict[str, Any]) -> dict[str, Any]:
    n = SYSTEMS[p["system"]]
    res = workdir / f"sim_res_S{n}_{int(TS * 1e6)}u.pkl"
    if not res.exists():
        raise RuntimeError(f"ParaEMT left no result file {res.name}")
    # The analysed window starts at the trip, as in the prototype campaign; a case with only a
    # governor step starts it at the step.
    t_first = p["t_trip"] if p["t_trip"] is not None else p["t_step"]
    cp = subprocess.run(
        [paraemt_python(), "-c", _EXTRACT, str(res), str(t_first)],
        cwd=workdir, env=_env(), capture_output=True, text=True, timeout=_timeout(),
    )
    if cp.returncode != 0:
        raise RuntimeError(f"ParaEMT result extraction failed: {cp.stderr[-600:]}")
    return json.loads(cp.stdout.strip().splitlines()[-1])


def summary_to_results(summary: dict[str, Any]) -> list[Result]:
    """Flatten the summary into (measurement, t, value) records for RESULTS_TOPIC (pure)."""
    results = []
    for name, time_key in SCALAR_METRICS.items():
        value = summary.get(name)
        if value is None:
            continue
        results.append(Result(name, float(summary[time_key]) if time_key else 0.0, float(value)))
    for bus, value in zip(summary.get("bus_num", []), summary.get("bus_vtm_min", [])):
        results.append(Result(f"vtm_min:bus{bus}", 0.0, float(value)))
    return results
