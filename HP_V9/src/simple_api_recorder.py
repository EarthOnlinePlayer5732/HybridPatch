"""Per-sample API success journals for the simplified V9 runtime."""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import uuid


SUCCESS_SCHEMA = "anchorpatch.simple_api_success/1"
FAILURE_SCHEMA = "anchorpatch.simple_api_failure/1"
TRANSPORT_REVISION = "opencode_openai_compatible/6"


class LocalCallEvidenceError(RuntimeError):
    """A deterministic call journal conflicts with the requested semantic call."""

    _anchorpatch_failure_class = "worker_failed"


def validate_complete_success_journal(
        payload, *, path=None, sample=None, method=None, rt_num=None,
        direction=None, call_leaf=None):
    """Validate one complete /6 success journal and its fixed-path identity."""
    if not isinstance(payload, dict) or payload.get("schema") != SUCCESS_SCHEMA:
        raise LocalCallEvidenceError("success journal schema is invalid")
    call_id = payload.get("call_id")
    request = payload.get("request")
    response = payload.get("response")
    request_sha = payload.get("request_sha256")
    if not isinstance(call_id, str) or not call_id:
        raise LocalCallEvidenceError("success journal call_id is missing")
    if (not isinstance(request, dict) or not isinstance(request_sha, str)
            or len(request_sha) != 64 or _sha256(request) != request_sha):
        raise LocalCallEvidenceError("success journal request identity is invalid")
    if not isinstance(response, dict):
        raise LocalCallEvidenceError("success journal response is invalid")
    if not isinstance(response.get("message"), str):
        raise LocalCallEvidenceError("success journal response message is missing")

    observed_sample = payload.get("sample")
    observed_method = payload.get("method")
    if (not isinstance(observed_sample, str) or not observed_sample
            or request.get("sample") != observed_sample):
        raise LocalCallEvidenceError("success journal sample identity is invalid")
    if (not isinstance(observed_method, str) or not observed_method
            or request.get("method") != observed_method):
        raise LocalCallEvidenceError("success journal method identity is invalid")
    if sample is not None and observed_sample != sample:
        raise LocalCallEvidenceError("success journal belongs to another sample")
    if method is not None and observed_method != method:
        raise LocalCallEvidenceError("success journal belongs to another method")

    parameters = request.get("parameters")
    call_kind = parameters.get("call_kind") if isinstance(parameters, dict) else None
    if not isinstance(call_kind, str) or not call_kind:
        raise LocalCallEvidenceError("success journal call kind is missing")
    expected_leaf = "repair" if call_kind == "hybridpatch_repair" else "primary"
    if call_leaf is not None and expected_leaf != call_leaf:
        raise LocalCallEvidenceError("success journal call kind/path mismatch")
    call_kinds = response.get("call_kinds")
    if not isinstance(call_kinds, list) or call_kinds != [call_kind]:
        raise LocalCallEvidenceError("success journal response call kind is invalid")

    if (response.get("transport_revision") != TRANSPORT_REVISION
            or response.get("transport") != "openai_sdk_stream"
            or response.get("http_status") != 200
            or response.get("stream_complete") is not True
            or not isinstance(response.get("finish_reason"), str)
            or not response.get("finish_reason").strip()
            or not isinstance(response.get("response_classification"), str)
            or not response.get("response_classification").strip()):
        raise LocalCallEvidenceError("success journal transport terminal is incomplete")
    attempts = response.get("transport_attempts")
    if not isinstance(attempts, list) or not attempts:
        raise LocalCallEvidenceError("success journal transport attempts are missing")
    if not all(isinstance(attempt, dict) for attempt in attempts):
        raise LocalCallEvidenceError("success journal transport attempt is invalid")
    terminal = attempts[-1]
    if (terminal.get("status") != "success"
            or terminal.get("http_status") != 200
            or terminal.get("stream_complete") is not True
            or terminal.get("final_usage_seen") is not True
            or terminal.get("terminal_sequence_valid") is not True):
        raise LocalCallEvidenceError("success journal terminal attempt is incomplete")

    if path is not None:
        journal_path = Path(path)
        try:
            observed_leaf = journal_path.parent.name
            observed_direction = journal_path.parent.parent.name
            observed_rt_text = journal_path.parent.parent.parent.name
            observed_rt = int(observed_rt_text.removeprefix("rt"))
        except (AttributeError, TypeError, ValueError) as exc:
            raise LocalCallEvidenceError(
                "success journal fixed path is invalid") from exc
        if (journal_path.name != "success.json"
                or observed_leaf not in {"primary", "repair"}
                or observed_direction not in {"forward", "backward"}
                or not observed_rt_text.startswith("rt")
                or observed_rt < 1):
            raise LocalCallEvidenceError("success journal fixed path is invalid")
        if rt_num is not None and observed_rt != rt_num:
            raise LocalCallEvidenceError("success journal belongs to another round trip")
        if direction is not None and observed_direction != direction:
            raise LocalCallEvidenceError("success journal belongs to another direction")
        if call_leaf is not None and observed_leaf != call_leaf:
            raise LocalCallEvidenceError("success journal belongs to another call leaf")
        if observed_leaf != expected_leaf:
            raise LocalCallEvidenceError("success journal call kind/path mismatch")
    return payload


def _utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _canonical_bytes(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")


def _sha256(value):
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _write_json_atomic(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _plain(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return _plain(to_dict())
        except Exception:
            pass
    return str(value)


def _exception_details(exc):
    last = getattr(exc, "last_error", None)
    status = getattr(exc, "status_code", None)
    if status is None:
        status = getattr(last, "status_code", None)
    body = getattr(last, "error_body", None)
    if body is None:
        body = getattr(last, "body", None)
    return {
        "error_type": f"{type(exc).__module__}.{type(exc).__qualname__}",
        "error_message": (str(exc) or repr(exc))[:4000],
        "http_status": status,
        "provider_error_type": getattr(last, "error_type", None),
        "provider_error_message": (
            getattr(last, "error_message", None) or str(last or ""))[:4000],
        "error_body": _plain(body),
        "transport_attempts": _plain(
            getattr(exc, "transport_attempts", None) or []),
    }


def classify_key_unusable(exc):
    """Return a narrow, explicit in-memory Key quarantine reason."""
    details = _exception_details(exc)
    status = details.get("http_status")
    text = " ".join(
        str(details.get(key) or "")
        for key in ("error_message", "provider_error_type",
                    "provider_error_message", "error_body")
    ).lower()
    if status in {401, 403}:
        return f"http_{status}_access_denied"
    if status == 402:
        return "http_402_payment_required"
    quota_markers = (
        "monthly usage limit", "quota exhausted", "usage limit reached",
        "insufficient balance", "insufficient credit", "billing limit",
        "payment required", "account balance", "quota exceeded",
    )
    if status == 429 and any(marker in text for marker in quota_markers):
        return "http_429_key_quota_exhausted"
    return None


class SimpleApiRecorder:
    """Replay complete calls and atomically publish new per-call journals."""

    def __init__(self, method_dir, sample, method, model, generate_impl=None):
        self.method_dir = Path(method_dir)
        self.sample = sample
        self.method = method
        self.model = model
        self._generate_impl = generate_impl
        self._step = None

    def set_step(self, rt_num, direction, _target_state_id=None):
        if not isinstance(rt_num, int) or rt_num < 1:
            raise ValueError("round trip must be a positive integer")
        if direction not in {"forward", "backward"}:
            raise ValueError("direction must be forward or backward")
        self._step = (rt_num, direction)

    def _call_dir(self, call_kind):
        if self._step is None:
            raise RuntimeError("API recorder step was not selected")
        rt_num, direction = self._step
        leaf = "repair" if call_kind == "hybridpatch_repair" else "primary"
        return (self.method_dir / "calls" / f"rt{rt_num:02d}" /
                direction / leaf)

    def _request(self, messages, kwargs):
        fields = {
            key: _plain(kwargs.get(key))
            for key in (
                "model", "timeout", "max_retries", "temperature", "is_json",
                "max_tokens", "thinking_mode", "reasoning_effort", "call_kind")
        }
        fields["model"] = fields.get("model") or self.model
        return {
            "sample": self.sample, "method": self.method,
            "messages": _plain(messages), "parameters": fields,
        }

    def _read_success(self, path, request_sha):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise LocalCallEvidenceError(
                f"success journal is not valid JSON: {path}") from exc
        rt_num, direction = self._step
        call_leaf = path.parent.name
        try:
            validate_complete_success_journal(
                payload, path=path, sample=self.sample, method=self.method,
                rt_num=rt_num, direction=direction, call_leaf=call_leaf)
        except LocalCallEvidenceError as exc:
            raise LocalCallEvidenceError(
                f"success journal identity mismatch: {path}: {exc}") from exc
        if payload.get("request_sha256") != request_sha:
            raise LocalCallEvidenceError(
                f"success journal identity mismatch: {path}")
        response = dict(payload["response"])
        response.setdefault("api_call_ids", [payload["call_id"]])
        response.setdefault("api_raw_paths", [
            path.relative_to(self.method_dir).as_posix()])
        response["replayed_from_success_journal"] = True
        return response

    def generate(self, messages, **kwargs):
        call_kind = kwargs.get("call_kind") or "primary"
        call_dir = self._call_dir(call_kind)
        success_path = call_dir / "success.json"
        request = self._request(messages, kwargs)
        request_sha = _sha256(request)
        if success_path.is_file():
            return self._read_success(success_path, request_sha)

        generate_impl = self._generate_impl
        if generate_impl is None:
            from model_openai import generate as generate_impl
        events = []

        def event_sink(event):
            events.append(_plain(event))

        try:
            response = generate_impl(
                messages, _raw_event_sink=event_sink, **kwargs)
        except Exception as exc:
            details = _exception_details(exc)
            failure_id = f"{dt.datetime.now().strftime('%Y%m%dT%H%M%S%f')}-{uuid.uuid4().hex[:8]}"
            failure_path = call_dir / "failures" / f"{failure_id}.json"
            _write_json_atomic(failure_path, {
                "schema": FAILURE_SCHEMA, "created_at": _utc_now(),
                "sample": self.sample, "method": self.method,
                "request_sha256": request_sha, "request": request,
                "failure": details, "transport_events": events,
            })
            try:
                exc._anchorpatch_simple_failure_path = str(failure_path)
                exc._anchorpatch_key_unusable_reason = classify_key_unusable(exc)
            except Exception:
                pass
            raise

        if not isinstance(response, dict):
            response = {"message": str(response)}
        call_id = uuid.uuid4().hex
        response = _plain(response)
        response["api_call_ids"] = [call_id]
        response["api_raw_paths"] = [
            success_path.relative_to(self.method_dir).as_posix()]
        response.setdefault("call_kinds", [call_kind])
        payload = {
            "schema": SUCCESS_SCHEMA, "created_at": _utc_now(),
            "call_id": call_id, "sample": self.sample, "method": self.method,
            "request_sha256": request_sha, "request": request,
            "response": response, "transport_events": events,
        }
        validate_complete_success_journal(
            payload, path=success_path, sample=self.sample, method=self.method,
            rt_num=self._step[0], direction=self._step[1],
            call_leaf=success_path.parent.name)
        _write_json_atomic(success_path, payload)
        return dict(response)


def scan_call_journals(method_dir):
    """Read per-call journals for summaries/verifiers without changing them."""
    method_dir = Path(method_dir)
    successes, failures = [], []
    calls_root = method_dir / "calls"
    if not calls_root.is_dir():
        return successes, failures
    for path in sorted(calls_root.rglob("*.json")):
        if path.name != "success.json" and path.parent.name != "failures":
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                payload = {
                    "parse_error": "journal top level is not a JSON object"}
        except Exception as exc:
            payload = {"path": str(path), "parse_error": str(exc)}
        payload["_path"] = str(path)
        if path.name == "success.json":
            successes.append(payload)
        else:
            failures.append(payload)
    return successes, failures
