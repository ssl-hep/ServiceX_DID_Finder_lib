# Copyright (c) 2019-25, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import json
import logging

import pytest

from servicex_did_finder_lib import logstash_logging
from servicex_did_finder_lib.logstash_logging import (
    LogstashFormatter,
    StreamFormatter,
    initialize_logging,
)


def make_record(msg="hello", extra=None, exc_info=None, level=logging.INFO):
    """Helper to build a LogRecord with optional extra attributes."""
    record = logging.LogRecord(
        name="test_logger",
        level=level,
        pathname="/path/to/module.py",
        lineno=42,
        msg=msg,
        args=None,
        exc_info=exc_info,
    )
    if extra:
        for k, v in extra.items():
            setattr(record, k, v)
    return record


@pytest.fixture
def clear_logging_cache():
    """initialize_logging is lru_cache'd, so clear it around each test."""
    initialize_logging.cache_clear()
    yield
    initialize_logging.cache_clear()


# ---------------------------------------------------------------------------
# StreamFormatter
# ---------------------------------------------------------------------------
def test_stream_formatter_no_extra():
    formatter = StreamFormatter("%(levelname)s %(message)s")
    record = make_record("a plain message")

    result = formatter.format(record)

    assert result == "INFO a plain message"
    assert "extra:" not in result


def test_stream_formatter_with_extra():
    formatter = StreamFormatter("%(levelname)s %(message)s")
    record = make_record("with extras", extra={"request_id": "abc123"})

    result = formatter.format(record)

    assert result.startswith("INFO with extras")
    assert "extra: " in result
    assert "request_id" in result
    assert "abc123" in result


# ---------------------------------------------------------------------------
# LogstashFormatter
# ---------------------------------------------------------------------------
def test_logstash_formatter_basic_fields(monkeypatch):
    monkeypatch.setattr(logstash_logging, "instance", "my-instance")
    formatter = LogstashFormatter(component_name="did-finder")
    record = make_record("logstash message")

    result = json.loads(formatter.format(record))

    assert result["message"] == "logstash message"
    assert result["path"] == "/path/to/module.py"
    assert result["instance"] == "my-instance"
    assert result["component"] == "did-finder"
    assert result["level"] == "INFO"
    assert result["@version"] == "1"
    assert "@timestamp" in result


def test_logstash_formatter_includes_extra_fields():
    formatter = LogstashFormatter(component_name="did-finder")
    record = make_record("msg", extra={"dataset": "ds1"})

    result = json.loads(formatter.format(record))

    assert result["dataset"] == "ds1"


def test_logstash_formatter_default_component_is_none():
    formatter = LogstashFormatter()
    record = make_record("msg")

    result = json.loads(formatter.format(record))

    assert result["component"] is None


# ---------------------------------------------------------------------------
# initialize_logging
# ---------------------------------------------------------------------------
def test_initialize_logging_returns_configured_logger(clear_logging_cache, monkeypatch):
    monkeypatch.delenv("LOGSTASH_HOST", raising=False)
    log = logging.getLogger("test_init_basic")
    log.handlers = []

    result = initialize_logging(log=log, component_name="did-finder")

    assert result is log
    assert result.level == logging.INFO
    assert result.propagate is False
    assert any(isinstance(h, logging.StreamHandler) for h in result.handlers)


def test_initialize_logging_default_logger(clear_logging_cache, monkeypatch):
    monkeypatch.delenv("LOGSTASH_HOST", raising=False)

    result = initialize_logging(component_name="did-finder")

    assert result.name == "servicex_did_finder_lib"


def test_initialize_logging_no_logstash_handler_without_host(
    clear_logging_cache, monkeypatch
):
    monkeypatch.delenv("LOGSTASH_HOST", raising=False)
    log = logging.getLogger("test_no_logstash")
    log.handlers = []

    result = initialize_logging(log=log, component_name="did-finder")

    assert len(result.handlers) == 1
    assert isinstance(result.handlers[0], logging.StreamHandler)


def test_initialize_logging_adds_logstash_handler_with_host(
    clear_logging_cache, monkeypatch
):
    monkeypatch.setenv("LOGSTASH_HOST", "logstash.example.com")
    monkeypatch.delenv("LOGSTASH_PORT", raising=False)

    created = {}

    class FakeHandler(logging.Handler):
        def __init__(self, host, port, version=0):
            super().__init__()
            created["host"] = host
            created["port"] = port
            created["version"] = version

        def emit(self, record):
            pass

    monkeypatch.setattr(logstash_logging.logstash, "TCPLogstashHandler", FakeHandler)

    log = logging.getLogger("test_with_logstash")
    log.handlers = []

    result = initialize_logging(log=log, component_name="did-finder")

    assert created["host"] == "logstash.example.com"
    assert created["port"] == 5959  # default port
    assert created["version"] == 1
    assert any(isinstance(h, FakeHandler) for h in result.handlers)


def test_initialize_logging_uses_custom_logstash_port(
    clear_logging_cache, monkeypatch
):
    monkeypatch.setenv("LOGSTASH_HOST", "logstash.example.com")
    monkeypatch.setenv("LOGSTASH_PORT", "9999")

    created = {}

    class FakeHandler(logging.Handler):
        def __init__(self, host, port, version=0):
            super().__init__()
            created["port"] = port

        def emit(self, record):
            pass

    monkeypatch.setattr(logstash_logging.logstash, "TCPLogstashHandler", FakeHandler)

    log = logging.getLogger("test_custom_port")
    log.handlers = []

    initialize_logging(log=log, component_name="did-finder")

    assert created["port"] == 9999
