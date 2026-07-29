"""Shared fixtures for the HP_V8 infrastructure regression suite."""

import argparse
import contextlib
import copy
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import threading
import traceback
import unittest
from datetime import datetime
from unittest import mock

import httpx
import portalocker
import hashlib

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
HERE = os.path.dirname(PACKAGE_DIR)
ROOT = os.path.dirname(HERE)
ARCHIVED_FIXTURE = os.path.join(
    HERE, "test_fixtures", "opencode_transport_v2_anomalies.json"
)
for path in (ROOT, HERE):
    if path not in sys.path:
        sys.path.insert(0, path)

import experiment_runner
import authorize_ledger_lock_recovery as ledger_recovery
import fr_baseline_dispatch
import model_openai
import paired_campaign_dispatch as paired_dispatch
import probe_fr_keys
import run_meta
import utils_relay_plan

from .fixture_builders import *


def _events(include_delta=True, include_stop=True, content=None):
    events = [{"type": "message_start"}]
    for index, block in enumerate(content or []):
        block_type = block.get("type")
        delta_type = "thinking_delta" if block_type == "thinking" else "text_delta"
        events.extend([
            {"type": "content_block_start", "index": index, "content_block": block},
            {"type": "content_block_delta", "index": index,
             "delta": {"type": delta_type}},
            {"type": "content_block_stop", "index": index},
        ])
    if include_delta:
        events.append({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 4},
        })
    if include_stop:
        events.append({"type": "message_stop"})
    return events


def _message(content, stop_reason="end_turn", usage=None):
    return {
        "id": "msg_test",
        "model": "minimax-m3",
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": usage or {
            "input_tokens": 5,
            "cache_read_input_tokens": 2,
            "cache_creation_input_tokens": 3,
            "output_tokens": 4,
        },
    }


def _archived_events(case):
    counts = case["events"]
    events = [{"type": "message_start"}]
    index = 0
    if counts.get("content_block_start_thinking"):
        events.append({
            "type": "content_block_start", "index": index,
            "content_block": {"type": "thinking", "thinking": ""},
        })
        events.extend({
            "type": "content_block_delta", "index": index,
            "delta": {"type": "thinking_delta", "thinking": "x"},
        } for _ in range(counts.get("thinking_delta", 0)))
        events.extend({
            "type": "content_block_delta", "index": index,
            "delta": {"type": "signature_delta", "signature": "x"},
        } for _ in range(counts.get("signature_delta", 0)))
        if counts.get("content_block_stop", 0) >= 1:
            events.append({"type": "content_block_stop", "index": index})
        index += 1
    if counts.get("content_block_start_text"):
        events.append({
            "type": "content_block_start", "index": index,
            "content_block": {"type": "text", "text": ""},
        })
        events.extend({
            "type": "content_block_delta", "index": index,
            "delta": {"type": "text_delta", "text": "x"},
        } for _ in range(counts.get("text_delta", 0)))
        if counts.get("content_block_stop", 0) >= 2:
            events.append({"type": "content_block_stop", "index": index})
    if counts.get("message_delta"):
        usage = case.get("usage") or {}
        events.append({
            "type": "message_delta",
            "delta": {"stop_reason": case.get("stop_reason")},
            "usage": (
                {"output_tokens": usage.get("output_tokens")}
                if usage.get("output_tokens") is not None else {}
            ),
        })
    if counts.get("message_stop"):
        events.append({"type": "message_stop"})
    return events


def _archived_message(case):
    content = []
    for block in case.get("content") or []:
        if block["type"] == "text":
            content.append({"type": "text", "text": "archived-text-placeholder"})
        else:
            content.append({"type": "thinking", "thinking": "archived-thinking-placeholder"})
    usage = case.get("usage") or {"input_tokens": None, "output_tokens": None}
    return _message(content, stop_reason=case.get("stop_reason"), usage=usage)


class _FakeStream:
    def __init__(self, events, final_message):
        self.events = list(events)
        self.final_message = final_message

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def __iter__(self):
        for event in self.events:
            if isinstance(event, BaseException):
                raise event
            yield event

    def get_final_message(self):
        return self.final_message


class _FakeMessages:
    def __init__(self, owner):
        self.owner = owner

    def stream(self, **kwargs):
        self.owner.request_body = kwargs
        return _FakeStream(self.owner.events, self.owner.final_message)


def _client_factory(events, final_message, captures):
    class FakeClient:
        def __init__(self, **kwargs):
            self.events = events
            self.final_message = final_message
            self.messages = _FakeMessages(self)
            captures["client_init"] = kwargs
            captures["client"] = self

        def close(self):
            captures["closed"] = True

    return FakeClient


def _successful_normalized(text="Hello", stop_reason="end_turn"):
    response = model_openai._normalize_anthropic_response(
        _message([{"type": "text", "text": text}], stop_reason=stop_reason)
    )
    response.update({
        "_transport_attempt": {
            "attempt_index": 1,
            "status": "success",
            "http_status": 200,
            "stream_complete": True,
            "message_start_seen": True,
            "message_delta_seen": True,
            "message_stop_seen": True,
            "final_usage_seen": True,
            "generation_delta_seen": True,
            "content_blocks_balanced": True,
        },
        "transport": "anthropic_sdk_v2",
        "transport_revision": "opencode_anthropic_sdk/4",
        "_raw_request_body": {},
        "_raw_stream_events": [],
    })
    return response



class _FakeOfficialResponse:
    def __init__(self, payload):
        self._payload = payload

    def model_dump(self):
        return self._payload


def _official_payload(content="Hello", finish_reason="stop"):
    return {
        "id": "chatcmpl-test",
        "choices": [{
            "finish_reason": finish_reason,
            "message": {
                "content": content,
                "reasoning_details": [{"type": "text", "text": "thinking..."}],
            },
        }],
        "usage": {"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14},
    }


def _stream_chunks_from_payload(payload):
    choice = (payload.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    common = {
        "id": payload.get("id"),
        "model": "deepseek-v4-flash",
    }
    chunks = [{
        **common,
        "choices": [{
            "index": 0,
            "delta": {
                "role": "assistant",
                "reasoning_details": message.get("reasoning_details") or [],
            },
            "finish_reason": None,
        }],
        "usage": None,
    }]
    if message.get("content") is not None:
        chunks.append({
            **common,
            "choices": [{
                "index": 0,
                "delta": {"content": message.get("content")},
                "finish_reason": None,
            }],
            "usage": None,
        })
    chunks.append({
        **common,
        "choices": [{
            "index": 0,
            "delta": {},
            "finish_reason": choice.get("finish_reason"),
        }],
        "usage": None,
    })
    chunks.append({
        **common,
        "choices": [],
        "usage": payload.get("usage"),
    })
    return chunks


def _official_client_factory(outcomes, captures):
    """outcomes: list of payload dicts or Exceptions, consumed per call."""

    class _Completions:
        def create(self, **kwargs):
            captures.append(kwargs)
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            if kwargs.get("stream"):
                chunks = (
                    outcome if isinstance(outcome, list)
                    else _stream_chunks_from_payload(outcome)
                )

                def _iter_chunks():
                    for chunk in chunks:
                        if isinstance(chunk, Exception):
                            raise chunk
                        yield chunk

                return _iter_chunks()
            return _FakeOfficialResponse(outcome)

    class _Chat:
        completions = _Completions()

    class _FakeOpenAI:
        def __init__(self, **kwargs):
            captures.append({"_ctor": kwargs})
            self.chat = _Chat()

    return _FakeOpenAI

__all__ = [name for name in globals() if not name.startswith("__")]
