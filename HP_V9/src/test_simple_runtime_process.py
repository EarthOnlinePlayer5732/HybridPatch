"""Self-contained zero-API process tests for the simplified V9 runtime."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent


SITECUSTOMIZE = r'''
import json
import os
from pathlib import Path
import time

import model_openai
import relay_core
import simple_runtime_io


def fake_generate(messages, **kwargs):
    delay = float(os.environ.get("SIMPLE_TEST_CALL_DELAY", "0"))
    if delay:
        time.sleep(delay)
    content = str((messages or [{}])[0].get("content") or "unknown")
    sample = content.split("|", 1)[0]
    log_root = Path(os.environ["SIMPLE_TEST_CALL_LOG"])
    log_root.mkdir(parents=True, exist_ok=True)
    with (log_root / f"{sample}.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"sample": sample, "kind": kwargs.get("call_kind")}) + "\n")
    attempt = {
        "status": "success", "http_status": 200, "stream_complete": True,
        "final_usage_seen": True, "terminal_sequence_valid": True,
        "retry_budget_consumed": False,
    }
    call_kind = kwargs.get("call_kind") or "fullrewrite_primary"
    return {
        "message": f"response:{content}", "http_status": 200,
        "stream_complete": True, "finish_reason": "stop",
        "response_classification": "normal",
        "transport": "openai_sdk_stream",
        "transport_revision": "opencode_openai_compatible/6",
        "reasoning_effort": "high", "call_kinds": [call_kind],
        "transport_attempts": [attempt], "prompt_tokens": 2,
        "completion_tokens": 1, "total_tokens": 3,
        "input_tokens": 2, "output_tokens": 1,
        "elapsed_time": delay, "retry_count": 0,
        "failed_attempt_count": 0,
    }


def fake_load_resume_state(out_dir, sample, method, *_args, **_kwargs):
    path = simple_runtime_io.method_dir(out_dir, sample, method) / "checkpoint.json"
    if not path.is_file():
        return None, 0
    checkpoint = simple_runtime_io.read_json(path)
    return checkpoint, int(checkpoint["completed_round_trips"])


def fake_run_method(method, sample_id, task_plan, *, num_round_trips, generate_fn,
                    hooks, resume_state=None, **_kwargs):
    start = int((resume_state or {}).get("completed_round_trips", 0))
    rid_chain = list((resume_state or {}).get("rid_chain") or [])
    state_chain = list((resume_state or {}).get("state_chain") or [])
    for rt in range(start + 1, num_round_trips + 1):
        rows = []
        for direction in ("forward", "backward"):
            target = task_plan[rt - 1] if direction == "forward" else "initial"
            hooks.call("set_step", rt, direction, target)
            response = generate_fn(
                [{"role": "user", "content": f"{sample_id}|{method}|{rt}|{direction}"}],
                model="deepseek-v4-flash", max_tokens=20000,
                return_metadata=True, timeout=1800, max_retries=3,
                thinking_mode="adaptive", reasoning_effort="high",
                call_kind=f"{method}_primary")
            exit_sample = os.environ.get("SIMPLE_TEST_EXIT_AFTER_CALL_ONCE")
            if sample_id == exit_sample and direction == "forward":
                marker = Path(os.environ["SIMPLE_TEST_CALL_LOG"]) / f"{sample_id}.exited"
                if not marker.exists():
                    marker.write_text("1", encoding="utf-8")
                    os._exit(77)
            rid = f"{sample_id}-{method}-{rt}-{direction}"
            rid_chain.append(rid)
            state_chain.append(target)
            rows.append({
                "sample_id": sample_id, "method": method,
                "round_trip_num": rt, "round_trip_direction": direction,
                "target_state_id": target, "task_state_id": target,
                "initial_state_id": "initial", "edit_instruction": f"edit:{target}",
                "raw_llm_response": response["message"], "response_id": rid,
                "rid_chain": list(rid_chain), "state_chain": list(state_chain),
                "evaluation": {"score": 1.0},
                "api_call_ids": response["api_call_ids"],
                "api_raw_paths": response["api_raw_paths"],
                "call_kinds": response["call_kinds"],
                "api_transport_attempts": response["transport_attempts"],
                "bdpatch": {"actual_method": "full_rewrite",
                            "preservation_violations": 0},
            })
        checkpoint = {
            "completed_round_trips": rt, "current_context": {"doc.txt": "ok"},
            "rid_chain": list(rid_chain), "state_chain": list(state_chain),
            "context_shuffle_random_state": [3, [1, 2, 3], None],
        }
        hooks.commit_round_trip(rows, checkpoint)
        hooks.call("log", {"round_trip": rt, "backward_score": 1.0})
    return {"sample": sample_id, "method": method,
            "completed_round_trips": num_round_trips}


model_openai.generate = fake_generate
simple_runtime_io.load_resume_state = fake_load_resume_state
relay_core.run_method = fake_run_method
'''


CAMPAIGN_WRAPPER = r'''
import json
import os
from pathlib import Path
import sys

import run_campaign
from simple_runtime_io import sha256_json, write_json_atomic

run_campaign._SAMPLES_ROOT = Path(os.environ["SIMPLE_TEST_SAMPLES_ROOT"])
run_campaign._validate_evaluators = lambda _samples: None

def fake_plans(out_dir, samples, round_trips, seed, _samples_root, *, allow_create=True):
    records = {}
    for sample in samples:
        payload = {
            "schema": "anchorpatch.simple_task_plan/1", "sample": sample,
            "round_trips": round_trips, "seed": seed,
            "target_state_ids": [f"target-{index}" for index in range(1, round_trips + 1)],
        }
        path = Path(out_dir) / "task_plans" / f"{sample}.json"
        if path.exists():
            if json.loads(path.read_text(encoding="utf-8")) != payload:
                raise RuntimeError(f"task plan differs for {sample}")
        elif allow_create:
            write_json_atomic(path, payload, overwrite=False)
        else:
            raise RuntimeError(f"task plan is missing for {sample}")
        records[sample] = {
            "path": f"task_plans/{sample}.json", "sha256": sha256_json(payload)}
    return records

run_campaign.prepare_task_plans = fake_plans
raise SystemExit(run_campaign.main(sys.argv[1:]))
'''


class SimpleRuntimeProcessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.out = self.base / "campaign"
        self.samples_root = self.base / "samples"
        self.calls = self.base / "call-log"
        self.harness = self.base / "harness"
        self.harness.mkdir()
        (self.harness / "sitecustomize.py").write_text(
            SITECUSTOMIZE, encoding="utf-8")
        self.keys = self.base / "keys.env"
        self.keys.write_text("KEY_1=fake-key-1\nKEY_2=fake-key-2\n", encoding="utf-8")
        self.processes = []

    def tearDown(self):
        for process in self.processes:
            if process.poll() is None:
                process.kill()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
            for stream in (process.stdout, process.stderr):
                if stream is not None and not stream.closed:
                    stream.close()
        self.temp.cleanup()

    def _make_samples(self, count):
        samples = [f"sample{index:03d}" for index in range(1, count + 1)]
        for sample in samples:
            folder = self.samples_root / sample
            folder.mkdir(parents=True)
            (folder / "sample.json").write_text("{}\n", encoding="utf-8")
        return samples

    def _env(self, **updates):
        env = os.environ.copy()
        env.update({
            "PYTHONUTF8": "1", "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": os.pathsep.join((str(self.harness), str(HERE))),
            "SIMPLE_TEST_SAMPLES_ROOT": str(self.samples_root),
            "SIMPLE_TEST_CALL_LOG": str(self.calls),
        })
        env.update({key: str(value) for key, value in updates.items()})
        return env

    def _command(self, samples, *, slots=30, all_samples=False):
        selection = ["--all"] if all_samples else ["--samples", *samples]
        return [
            sys.executable, "-u", "-B", "-c", CAMPAIGN_WRAPPER,
            *selection, "--methods", "fullrewrite", "--round-trips", "1",
            "--keys-file", str(self.keys), "--key-labels", "KEY_1", "KEY_2",
            "--slots-per-key", str(slots), "--out-dir", str(self.out),
            "--grace-seconds", "2", "--log-level", "quiet",
        ]

    def _start(self, samples, *, env=None, slots=30, all_samples=False):
        kwargs = {}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        process = subprocess.Popen(
            self._command(samples, slots=slots, all_samples=all_samples),
            cwd=ROOT, env=env or self._env(), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, **kwargs)
        self.processes.append(process)
        return process

    def _run(self, samples, *, env=None, slots=30, all_samples=False, timeout=60):
        process = self._start(
            samples, env=env, slots=slots, all_samples=all_samples)
        stdout, stderr = process.communicate(timeout=timeout)
        self.assertEqual(process.returncode, 0, (stdout, stderr))
        return process

    def _dispatch_rows(self):
        path = self.out / "dispatch.jsonl"
        if not path.is_file():
            return []
        return [json.loads(line) for line in path.read_text(
            encoding="utf-8").splitlines() if line.strip()]

    def _wait_for(self, predicate, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        self.fail("timed out waiting for process evidence")

    def test_three_real_workers_complete_with_fake_provider(self):
        samples = self._make_samples(3)
        self._run(samples)
        for sample in samples:
            status = json.loads((
                self.out / "samples" / sample / "status.json").read_text(
                    encoding="utf-8"))
            self.assertEqual(status["state"], "complete")
            self.assertEqual(len((self.calls / f"{sample}.jsonl").read_text(
                encoding="utf-8").splitlines()), 2)

    def test_killed_worker_does_not_stop_other_samples(self):
        samples = self._make_samples(3)
        process = self._start(
            samples, env=self._env(SIMPLE_TEST_CALL_DELAY="2"), slots=3)
        self._wait_for(lambda: len([
            row for row in self._dispatch_rows()
            if row.get("event") == "worker_started"]) == 3)
        victim = next(
            row for row in self._dispatch_rows()
            if row.get("event") == "worker_started"
            and row.get("sample") == samples[0])
        os.kill(int(victim["pid"]), signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=30)
        self.assertEqual(process.returncode, 2, (stdout, stderr))
        for sample in samples[1:]:
            status = json.loads((
                self.out / "samples" / sample / "status.json").read_text(
                    encoding="utf-8"))
            self.assertEqual(status["state"], "complete")

    def test_sigint_then_same_command_resumes(self):
        samples = self._make_samples(3)
        process = self._start(
            samples, env=self._env(SIMPLE_TEST_CALL_DELAY="2"), slots=3)
        self._wait_for(lambda: len([
            row for row in self._dispatch_rows()
            if row.get("event") == "worker_started"]) == 3)
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.send_signal(signal.SIGINT)
        stdout, stderr = process.communicate(timeout=30)
        self.assertIn(process.returncode, {130, 143}, (stdout, stderr))
        self._run(samples, env=self._env(), slots=3)
        self.assertTrue(all(
            json.loads((self.out / "samples" / sample / "status.json").read_text(
                encoding="utf-8"))["state"] == "complete"
            for sample in samples))

    def test_dispatcher_kill_does_not_duplicate_live_sample_worker(self):
        samples = self._make_samples(1)
        first = self._start(
            samples, env=self._env(SIMPLE_TEST_CALL_DELAY="1"), slots=1)
        self._wait_for(lambda: any(
            row.get("event") == "worker_started" for row in self._dispatch_rows()))
        first.kill()
        first.communicate(timeout=10)
        self._run(samples, env=self._env(SIMPLE_TEST_CALL_DELAY="0"), slots=1)
        starts = [row for row in self._dispatch_rows()
                  if row.get("event") == "worker_started"]
        self.assertEqual(len(starts), 1)

    def test_success_journal_without_commit_is_replayed_after_restart(self):
        samples = self._make_samples(1)
        env = self._env(SIMPLE_TEST_EXIT_AFTER_CALL_ONCE=samples[0])
        first = self._start(samples, env=env, slots=1)
        stdout, stderr = first.communicate(timeout=30)
        self.assertEqual(first.returncode, 2, (stdout, stderr))
        self._run(samples, env=env, slots=1)
        calls = (self.calls / f"{samples[0]}.jsonl").read_text(
            encoding="utf-8").splitlines()
        self.assertEqual(len(calls), 2)

    def test_thirty_real_workers_write_only_disjoint_sample_roots(self):
        samples = self._make_samples(30)
        self._run(samples, slots=30, timeout=90)
        starts = [row for row in self._dispatch_rows()
                  if row.get("event") == "worker_started"]
        self.assertEqual(len(starts), 30)
        self.assertEqual({row["sample"] for row in starts}, set(samples))
        for forbidden in (
                "api_calls.jsonl", "api_attempt_ledger.jsonl",
                "sample_outcomes.jsonl", "run_metadata_events.jsonl"):
            self.assertFalse((self.out / forbidden).exists())


if __name__ == "__main__":
    unittest.main()
