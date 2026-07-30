"""Small, sample-local persistence layer for the simplified V9 runtime."""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import uuid

import portalocker

from relay_core import _editable, _run_attempt_hybrid
from utils_context import build_context_from_folder, parse_context_string
from utils_env import (load_distractor_context, load_sample, merge_distractor,
                       shuffle_context)
from utils_relay_plan import build_relay_task_plan


RUN_SCHEMA = "anchorpatch.simple_campaign/1"
STATUS_SCHEMA = "anchorpatch.simple_sample_status/1"
DISPATCH_SCHEMA = "anchorpatch.simple_dispatch_event/1"
ALLOWED_SAMPLE_STATES = {
    "pending", "running", "complete", "api_incomplete", "evaluator_failed",
    "preservation_invalid", "worker_failed",
}
ALLOWED_METHOD_STATES = {"not_started", "running", "complete", "incomplete"}


class LocalEvidenceError(RuntimeError):
    """One sample's result/checkpoint evidence cannot be resumed safely."""

    _anchorpatch_failure_class = "worker_failed"


class LockUnavailableError(RuntimeError):
    pass


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical_bytes(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")


def sha256_json(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def read_json(path):
    path = Path(path)
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_json_atomic(path, payload, *, overwrite=True):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and path.exists():
        raise FileExistsError(path)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if not overwrite and path.exists():
            raise FileExistsError(path)
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def append_jsonl_owned(path, payload):
    """Append to a file whose single writer is fixed by the runtime contract."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


class ProcessFileLock:
    """An OS-released non-reentrant lock with a persistent path."""

    def __init__(self, path, *, label):
        self.path = Path(path)
        self.label = label
        self.handle = None

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            portalocker.lock(handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except portalocker.exceptions.LockException as exc:
            handle.close()
            raise LockUnavailableError(f"{self.label} is already held") from exc
        self.handle = handle
        return self

    def close(self):
        if self.handle is not None:
            try:
                portalocker.unlock(self.handle)
            finally:
                self.handle.close()
                self.handle = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *_args):
        self.close()


def campaign_lock(out_dir):
    return ProcessFileLock(Path(out_dir) / ".campaign.lock", label="campaign lock")


def sample_lock(out_dir, sample):
    return ProcessFileLock(
        Path(out_dir) / "locks" / f"{sample}.lock",
        label=f"sample lock for {sample}")


def lock_is_held(path):
    lock = ProcessFileLock(path, label=str(path))
    try:
        lock.acquire()
    except LockUnavailableError:
        return True
    else:
        lock.close()
        return False


def git_identity(repo_root):
    def run(*args):
        result = subprocess.run(
            ["git", *args], cwd=str(repo_root), text=True,
            encoding="utf-8", stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, check=True)
        return result.stdout.strip()
    commit = run("rev-parse", "HEAD")
    dirty = bool(run("status", "--porcelain", "--untracked-files=normal"))
    return commit, "dirty" if dirty else "clean"


def load_keys(path, labels):
    path = Path(path).resolve()
    if not path.is_file():
        raise RuntimeError(f"keys file not found: {path}")
    parsed = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            label, value = (part.strip() for part in line.split("=", 1))
            if label and value:
                if label in parsed:
                    raise RuntimeError(f"duplicate Key label: {label}")
                parsed[label] = value
    requested = list(labels or [])
    if len(requested) != len(set(requested)):
        raise RuntimeError("--key-labels contains duplicates")
    missing = [label for label in requested if not parsed.get(label)]
    if missing:
        raise RuntimeError(f"missing or empty Key labels: {missing}")
    selected = {label: parsed[label] for label in requested}
    if not selected:
        raise RuntimeError("at least one non-empty Key is required")
    if len(set(selected.values())) != len(selected):
        raise RuntimeError("selected Key values must be unique")
    return selected


def all_sample_ids(samples_root):
    root = Path(samples_root)
    return sorted(
        child.name for child in root.iterdir()
        if child.is_dir() and (child / "sample.json").is_file())


def validate_samples(samples, samples_root):
    samples = list(samples)
    if len(samples) != len(set(samples)):
        raise RuntimeError("sample list contains duplicates")
    known = set(all_sample_ids(samples_root))
    missing = [sample for sample in samples if sample not in known]
    if missing:
        raise RuntimeError(f"unknown samples: {missing}")
    return samples


def method_orders(samples, methods):
    methods = list(methods)
    if len(methods) != len(set(methods)):
        raise RuntimeError("method list contains duplicates")
    if not methods or not set(methods) <= {"hybridpatch", "fullrewrite"}:
        raise RuntimeError("methods must contain hybridpatch and/or fullrewrite")
    if set(methods) == {"hybridpatch", "fullrewrite"}:
        return {
            sample: (["hybridpatch", "fullrewrite"] if index % 2 == 0
                     else ["fullrewrite", "hybridpatch"])
            for index, sample in enumerate(samples)
        }
    return {sample: list(methods) for sample in samples}


def _task_plan_payload(sample, plan, round_trips, seed):
    return {
        "schema": "anchorpatch.simple_task_plan/1", "sample": sample,
        "round_trips": round_trips, "seed": seed,
        "target_state_ids": list(plan),
    }


def prepare_task_plans(out_dir, samples, round_trips, seed, samples_root, *,
                       allow_create=True):
    out_dir = Path(out_dir)
    records = {}
    for sample in samples:
        loaded, _folder, states = load_sample(
            sample, samples_folder=str(Path(samples_root)) + os.sep)
        initial = states[loaded["start_state"]]
        possible = [item["target_state"] for item in initial["prompts"]]
        payload = _task_plan_payload(
            sample, build_relay_task_plan(possible, round_trips, seed=seed),
            round_trips, seed)
        path = out_dir / "task_plans" / f"{sample}.json"
        if path.exists():
            if read_json(path) != payload:
                raise RuntimeError(f"task plan differs for {sample}")
        else:
            if not allow_create:
                raise RuntimeError(f"task plan is missing for {sample}")
            write_json_atomic(path, payload, overwrite=False)
        records[sample] = {
            "path": f"task_plans/{sample}.json",
            "sha256": sha256_json(payload),
        }
    return records


def load_task_plan(out_dir, sample):
    """Read and authenticate one immutable task plan from run.json."""
    out_dir = Path(out_dir)
    run = read_json(out_dir / "run.json")
    scientific = run.get("scientific")
    if not isinstance(scientific, dict):
        raise LocalEvidenceError("run.json scientific configuration is invalid")
    record = (scientific.get("task_plans") or {}).get(sample)
    if not isinstance(record, dict) or not isinstance(record.get("path"), str):
        raise LocalEvidenceError(f"task plan record is missing for {sample}")
    payload = read_json(out_dir / record["path"])
    if (not isinstance(payload, dict)
            or payload.get("schema") != "anchorpatch.simple_task_plan/1"
            or payload.get("sample") != sample
            or sha256_json(payload) != record.get("sha256")):
        raise LocalEvidenceError(f"task plan identity mismatch for {sample}")
    targets = payload.get("target_state_ids")
    if (not isinstance(targets, list)
            or not all(isinstance(item, str) and item for item in targets)):
        raise LocalEvidenceError(f"task plan targets are invalid for {sample}")
    return list(targets), payload


def create_or_validate_run(out_dir, scientific, execution):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    probe = out_dir / f".write-probe-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as exc:
        raise RuntimeError(f"out_dir is not writable: {out_dir}") from exc
    path = out_dir / "run.json"
    if path.exists():
        run = read_json(path)
        if run.get("schema") != RUN_SCHEMA or run.get("scientific") != scientific:
            raise RuntimeError(
                "existing run.json scientific configuration differs; "
                "use a new out_dir")
        return run, False
    payload = {
        "schema": RUN_SCHEMA, "created_at": utc_now(),
        "initial_state": "new", "scientific": scientific,
        "initial_execution": execution,
    }
    write_json_atomic(path, payload, overwrite=False)
    return payload, True


def sample_dir(out_dir, sample):
    return Path(out_dir) / "samples" / sample


def method_dir(out_dir, sample, method):
    return sample_dir(out_dir, sample) / method


def default_status(sample, methods):
    return {
        "schema": STATUS_SCHEMA, "sample": sample, "state": "pending",
        "methods": {method: "not_started" for method in methods},
        "updated_at": utc_now(), "attempt": 0,
    }


def read_status(out_dir, sample, methods):
    path = sample_dir(out_dir, sample) / "status.json"
    if not path.exists():
        return default_status(sample, methods)
    try:
        payload = read_json(path)
    except Exception as exc:
        raise LocalEvidenceError(
            f"sample status is unreadable for {sample}") from exc
    if (payload.get("schema") != STATUS_SCHEMA
            or payload.get("sample") != sample
            or payload.get("state") not in ALLOWED_SAMPLE_STATES):
        raise LocalEvidenceError(f"sample status is invalid for {sample}")
    method_states = payload.get("methods")
    if (not isinstance(method_states, dict)
            or set(method_states) != set(methods)
            or not set(method_states.values()) <= ALLOWED_METHOD_STATES):
        raise LocalEvidenceError(f"method status is invalid for {sample}")
    return payload


def write_status(out_dir, sample, payload):
    if payload.get("state") not in ALLOWED_SAMPLE_STATES:
        raise ValueError("invalid sample state")
    payload = dict(payload)
    payload.update(schema=STATUS_SCHEMA, sample=sample, updated_at=utc_now())
    write_json_atomic(sample_dir(out_dir, sample) / "status.json", payload)
    return payload


def read_result_rows(path):
    path = Path(path)
    rows = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception as exc:
                raise LocalEvidenceError(
                    f"invalid JSON result row {path}:{line_number}") from exc
            rows.append(row)
    return rows


def validate_result_pairs(rows, round_trips):
    by_rt = {}
    for index, row in enumerate(rows):
        rt = row.get("round_trip_num")
        direction = row.get("round_trip_direction")
        if (not isinstance(rt, int) or isinstance(rt, bool) or rt < 1
                or rt > round_trips or direction not in {"forward", "backward"}):
            raise LocalEvidenceError("result row has an invalid round-trip key")
        expected = (index // 2 + 1, "forward" if index % 2 == 0 else "backward")
        if (rt, direction) != expected:
            raise LocalEvidenceError(
                f"result row order mismatch: expected RT{expected[0]}/"
                f"{expected[1]}, found RT{rt}/{direction}")
        slot = by_rt.setdefault(rt, {})
        if direction in slot:
            raise LocalEvidenceError(f"duplicate result row for RT{rt}/{direction}")
        slot[direction] = row
    complete = 0
    for rt in range(1, round_trips + 1):
        slot = by_rt.get(rt)
        if slot is None:
            if any(index > rt for index in by_rt):
                raise LocalEvidenceError(f"result rows skip RT{rt}")
            break
        if set(slot) != {"forward", "backward"}:
            raise LocalEvidenceError(f"result rows contain a partial RT{rt}")
        complete = rt
    return by_rt, complete


def _transition_prompt(source_state, target_state_id):
    matches = [
        item.get("prompt") for item in source_state.get("prompts") or []
        if isinstance(item, dict) and item.get("target_state") == target_state_id]
    if len(matches) != 1 or not isinstance(matches[0], str):
        raise LocalEvidenceError(
            f"frozen transition prompt is missing or ambiguous for {target_state_id}")
    return matches[0]


def validate_result_trajectory(rows, sample, method, round_trips, task_plan,
                               samples_root):
    """Bind committed rows to the immutable sample task plan and prompts."""
    by_rt, complete = validate_result_pairs(rows, round_trips)
    if len(task_plan) < round_trips:
        raise LocalEvidenceError("task plan is shorter than requested round trips")
    loaded, _sample_folder, states = load_sample(
        sample, samples_folder=str(Path(samples_root)) + os.sep)
    initial_id = loaded["start_state"]
    initial = states[initial_id]
    expected_state_chain = []
    for rt in range(1, complete + 1):
        forward_id = task_plan[rt - 1]
        if forward_id not in states:
            raise LocalEvidenceError(
                f"task plan RT{rt} target state is unknown: {forward_id}")
        forward_state = states[forward_id]
        expected = {
            "forward": (
                forward_id, _transition_prompt(initial, forward_id)),
            "backward": (
                initial_id, _transition_prompt(forward_state, initial_id)),
        }
        for direction in ("forward", "backward"):
            row = by_rt[rt][direction]
            target_id, instruction = expected[direction]
            if (row.get("sample_id") != sample or row.get("method") != method
                    or row.get("target_state_id") != target_id
                    or row.get("task_state_id") != target_id
                    or row.get("initial_state_id") != initial_id
                    or row.get("edit_instruction") != instruction):
                raise LocalEvidenceError(
                    f"result trajectory differs from task plan at "
                    f"RT{rt}/{direction}")
            expected_state_chain.append(target_id)
            state_chain = row.get("state_chain")
            rid_chain = row.get("rid_chain")
            expected_length = len(expected_state_chain)
            if (state_chain != expected_state_chain
                    or not isinstance(rid_chain, list)
                    or len(rid_chain) != expected_length
                    or not all(isinstance(item, str) and item for item in rid_chain)
                    or row.get("response_id") != rid_chain[-1]):
                raise LocalEvidenceError(
                    f"result relay chain is invalid at RT{rt}/{direction}")
    return by_rt, complete, expected_state_chain


def validate_checkpoint_state(checkpoint, complete, by_rt, expected_state_chain):
    """Check the hot-path checkpoint shape and its terminal row binding."""
    if checkpoint is None:
        if complete:
            return
        return
    if not isinstance(checkpoint, dict):
        raise LocalEvidenceError("checkpoint must be a JSON object")
    checkpoint_rt = checkpoint.get("completed_round_trips")
    if (not isinstance(checkpoint_rt, int) or isinstance(checkpoint_rt, bool)
            or checkpoint_rt < 0 or checkpoint_rt > complete):
        raise LocalEvidenceError("checkpoint round-trip progress is invalid")
    expected_length = checkpoint_rt * 2
    rid_chain = checkpoint.get("rid_chain")
    state_chain = checkpoint.get("state_chain")
    if (not isinstance(checkpoint.get("current_context"), dict)
            or not isinstance(rid_chain, list)
            or not isinstance(state_chain, list)
            or len(rid_chain) != expected_length
            or len(state_chain) != expected_length
            or state_chain != expected_state_chain[:expected_length]
            or not all(isinstance(item, str) and item for item in rid_chain)
            or not isinstance(
                checkpoint.get("context_shuffle_random_state"), (list, tuple))):
        raise LocalEvidenceError("checkpoint relay state is invalid")
    if checkpoint_rt:
        terminal = by_rt[checkpoint_rt]["backward"]
        if (rid_chain != terminal.get("rid_chain")
                or state_chain != terminal.get("state_chain")):
            raise LocalEvidenceError("checkpoint does not match its terminal result row")


def commit_round_trip(method_path, rows, checkpoint):
    """Append one sample-local pair and atomically advance its checkpoint."""
    method_path = Path(method_path)
    result_path = method_path / "result.jsonl"
    checkpoint_path = method_path / "checkpoint.json"
    existing = read_result_rows(result_path)
    _by_rt, complete = validate_result_pairs(
        existing, int(checkpoint["completed_round_trips"]))
    target = int(checkpoint["completed_round_trips"])
    if complete >= target:
        return {"status": "already_committed", "round_trip": target}
    if complete != target - 1:
        raise LocalEvidenceError("result progress is not aligned before commit")
    keys = [(row.get("round_trip_num"), row.get("round_trip_direction"))
            for row in rows]
    if keys != [(target, "forward"), (target, "backward")]:
        raise LocalEvidenceError("round-trip commit is not a forward/backward pair")
    method_path.mkdir(parents=True, exist_ok=True)
    with result_path.open("a", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    write_json_atomic(checkpoint_path, checkpoint)
    return {"status": "appended", "round_trip": target, "rows_appended": 2}


def _tuple_tree(value):
    return tuple(_tuple_tree(item) for item in value) if isinstance(value, list) else value


def _replay_generated(method, row, context, distractor, target_state):
    input_real = _editable(context, distractor)
    if method == "fullrewrite":
        return parse_context_string(row.get("raw_llm_response") or "")
    actual = ((row.get("bdpatch") or {}).get("actual_method"))
    if actual == "hybridpatch_protocol_failure_kept_context":
        return dict(input_real)
    telemetry = ((row.get("bdpatch") or {}).get("hybrid") or {})
    attempt = _run_attempt_hybrid(
        row.get("raw_llm_response") or "", input_real,
        list(target_state["context"]), list(distractor),
        edit_instruction=row.get("edit_instruction"),
        require_effective_change=row.get("round_trip_direction") == "forward")
    generated = attempt.get("gen")
    if not generated:
        raise LocalEvidenceError("committed HybridPatch row cannot be replayed")
    if not attempt.get("gate_pass") and not telemetry.get("partial_acceptance"):
        raise LocalEvidenceError("committed HybridPatch row fails replay gate")
    return generated


def load_resume_state(out_dir, sample, method, round_trips, seed,
                      include_distractor, samples_root, task_plan=None):
    """Validate local evidence and repair only a checkpoint lagging full pairs."""
    path = method_dir(out_dir, sample, method)
    rows = read_result_rows(path / "result.jsonl")
    if task_plan is None:
        task_plan, _payload = load_task_plan(out_dir, sample)
    by_rt, complete, expected_state_chain = validate_result_trajectory(
        rows, sample, method, round_trips, task_plan, samples_root)
    checkpoint_path = path / "checkpoint.json"
    checkpoint = read_json(checkpoint_path) if checkpoint_path.exists() else None
    validate_checkpoint_state(checkpoint, complete, by_rt, expected_state_chain)
    checkpoint_rt = checkpoint.get("completed_round_trips", 0) if checkpoint else 0
    if checkpoint_rt > complete:
        raise LocalEvidenceError("checkpoint is ahead of result rows")
    if checkpoint_rt == complete:
        return checkpoint, complete

    random.seed(seed)
    loaded, sample_folder, states = load_sample(
        sample, samples_folder=str(Path(samples_root)) + os.sep)
    initial_id = loaded["start_state"]
    initial = states[initial_id]
    distractor = load_distractor_context(sample_folder) if include_distractor else {}
    if checkpoint:
        context = checkpoint["current_context"]
        rid_chain = list(checkpoint["rid_chain"])
        state_chain = list(checkpoint["state_chain"])
        random.setstate(_tuple_tree(checkpoint["context_shuffle_random_state"]))
    else:
        context = build_context_from_folder(
            os.path.join(sample_folder, initial["solution_folder"]))
        if include_distractor:
            context = merge_distractor(context, distractor)
        context = shuffle_context(context)
        rid_chain, state_chain = [], []
    for rt in range(checkpoint_rt + 1, complete + 1):
        for direction in ("forward", "backward"):
            row = by_rt[rt][direction]
            target = states[row["target_state_id"]]
            generated = _replay_generated(
                method, row, context, distractor, target)
            context = shuffle_context(merge_distractor(generated, distractor))
        rid_chain = list(by_rt[rt]["backward"].get("rid_chain") or rid_chain)
        state_chain = list(by_rt[rt]["backward"].get("state_chain") or state_chain)
    checkpoint = {
        "completed_round_trips": complete, "current_context": context,
        "rid_chain": rid_chain, "state_chain": state_chain,
        "context_shuffle_random_state": random.getstate(),
    }
    validate_checkpoint_state(
        checkpoint, complete, by_rt, expected_state_chain)
    write_json_atomic(checkpoint_path, checkpoint)
    return checkpoint, complete
