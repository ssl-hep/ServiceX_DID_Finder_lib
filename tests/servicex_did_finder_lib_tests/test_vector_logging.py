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
import sys

import pytest

from servicex_did_finder_lib import vector_logging
from servicex_did_finder_lib.vector_logging import (
    VectorFormatter,
    VectorQueueHandler,
    initialize_vector_logging,
)

# Columns of the Postgres log_messages table that the vector aggregator inserts
# into. A key outside this set fails the insert, and it fails silently -- the
# row just never appears -- so pin the set.
LOG_MESSAGE_COLUMNS = {
    "timestamp",
    "level",
    "logger",
    "instance",
    "component",
    "message",
    "dataset_id",
    "extra",
}


def make_record(msg="hello", extra=None, exc_info=None, level=logging.INFO):
    """Helper to build a LogRecord with optional extra attributes."""
    record = logging.LogRecord(
        name="servicex_did_finder_lib.did_finder_app",
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


def format_record(record, component_name="rucio_did_finder"):
    formatted = VectorFormatter(component_name=component_name).format(record)
    return json.loads(formatted.decode("utf-8"))


class FakeSink(logging.Handler):
    """Stands in for logstash.TCPLogstashHandler so nothing opens a socket."""

    instances = []

    def __init__(self, host, port, version=0):
        super().__init__()
        self.host = host
        self.port = port
        self.version = version
        self.sock = None
        FakeSink.instances.append(self)

    def emit(self, record):
        pass


@pytest.fixture
def fake_sink(monkeypatch):
    FakeSink.instances = []
    monkeypatch.setattr(vector_logging.logstash, "TCPLogstashHandler", FakeSink)
    yield FakeSink
    FakeSink.instances = []


@pytest.fixture
def vector_env(monkeypatch):
    monkeypatch.setenv("VECTOR_HOST", "vector-aggregator")
    monkeypatch.setenv("VECTOR_PORT", "9000")


@pytest.fixture
def closing_logger():
    """Hand out loggers and close their handlers, so no listener thread leaks."""
    loggers = []

    def _make(name):
        log = logging.getLogger(name)
        log.handlers = []
        log.setLevel(logging.INFO)
        loggers.append(log)
        return log

    yield _make

    for log in loggers:
        for handler in list(log.handlers):
            handler.close()
        log.handlers = []


# ---------------------------------------------------------------------------
# VectorFormatter
# ---------------------------------------------------------------------------
def test_vector_formatter_emits_exactly_the_log_message_columns():
    event = format_record(make_record(extra={"dataset_id": 17}))

    assert set(event) == LOG_MESSAGE_COLUMNS


def test_vector_formatter_basic_fields(monkeypatch):
    monkeypatch.setattr(vector_logging, "instance", "my-instance")

    event = format_record(make_record("a lookup happened"))

    assert event["message"] == "a lookup happened"
    assert event["level"] == "INFO"
    assert event["logger"] == "servicex_did_finder_lib.did_finder_app"
    assert event["instance"] == "my-instance"
    assert event["component"] == "rucio_did_finder"
    assert "timestamp" in event


def test_vector_formatter_hoists_dataset_id_out_of_extra():
    event = format_record(make_record(extra={"dataset_id": 42, "num_files": 3}))

    assert event["dataset_id"] == 42
    assert "dataset_id" not in event["extra"]
    assert event["extra"]["num_files"] == 3


def test_vector_formatter_dataset_id_is_null_when_absent():
    event = format_record(make_record())

    assert event["dataset_id"] is None


def test_vector_formatter_omits_request_id_column():
    """A DID lookup belongs to a dataset, not a request; the column stays NULL."""
    event = format_record(make_record())

    assert "request_id" not in event


def test_vector_formatter_component_is_per_finder():
    event = format_record(make_record(), component_name="cernopendata_did_finder")

    assert event["component"] == "cernopendata_did_finder"


def test_vector_formatter_puts_debug_fields_in_extra():
    try:
        raise ValueError("lookup blew up")
    except ValueError:
        record = make_record("boom", exc_info=sys.exc_info(), level=logging.ERROR)

    event = format_record(record)

    assert set(event) == LOG_MESSAGE_COLUMNS
    assert "stack_trace" in event["extra"]
    assert "ValueError" in event["extra"]["stack_trace"]


# ---------------------------------------------------------------------------
# VectorQueueHandler
# ---------------------------------------------------------------------------
def test_queue_handler_starts_a_listener_on_first_emit(fake_sink):
    handler = VectorQueueHandler("vector-aggregator", "9000", "x_did_finder", logging.INFO)
    try:
        assert not fake_sink.instances

        handler.emit(make_record())

        assert len(fake_sink.instances) == 1
        assert fake_sink.instances[0].host == "vector-aggregator"
        assert fake_sink.instances[0].port == 9000
    finally:
        handler.close()


def test_queue_handler_recreates_listener_after_pid_change(fake_sink, monkeypatch):
    """
    Celery's prefork pool forks after logging is initialized. The listener is a
    thread, so it does not survive the fork -- without this the children would
    enqueue into a queue nobody drains and log_messages would stay empty.
    """
    handler = VectorQueueHandler("vector-aggregator", "9000", "x_did_finder", logging.INFO)
    try:
        handler.emit(make_record())
        parent_queue = handler.queue
        parent_listener = handler._listener

        # Pretend we are now inside a forked child.
        monkeypatch.setattr(vector_logging.os, "getpid", lambda: 999999)
        handler.emit(make_record())

        assert handler.queue is not parent_queue
        assert handler._listener is not parent_listener
        assert len(fake_sink.instances) == 2
    finally:
        handler.close()


def test_queue_handler_reuses_listener_within_one_process(fake_sink):
    handler = VectorQueueHandler("vector-aggregator", "9000", "x_did_finder", logging.INFO)
    try:
        handler.emit(make_record())
        handler.emit(make_record())
        handler.emit(make_record())

        assert len(fake_sink.instances) == 1
    finally:
        handler.close()


def test_queue_handler_closes_the_inherited_socket(fake_sink, monkeypatch):
    """The child must not keep writing down the fd it shares with its parent."""
    closed = []

    class TrackingSink(FakeSink):
        def __init__(self, host, port, version=0):
            super().__init__(host, port, version)
            self.sock = type("Sock", (), {"close": lambda _self: closed.append(True)})()

    monkeypatch.setattr(vector_logging.logstash, "TCPLogstashHandler", TrackingSink)

    handler = VectorQueueHandler("vector-aggregator", "9000", "x_did_finder", logging.INFO)
    try:
        handler.emit(make_record())
        monkeypatch.setattr(vector_logging.os, "getpid", lambda: 999999)
        handler.emit(make_record())

        assert closed == [True]
    finally:
        handler.close()


# ---------------------------------------------------------------------------
# initialize_vector_logging
# ---------------------------------------------------------------------------
def test_initialize_vector_logging_noop_without_host(closing_logger):
    log = closing_logger("test_vector_no_host")

    initialize_vector_logging(log, "x_did_finder")

    assert log.handlers == []


def test_initialize_vector_logging_noop_without_port(closing_logger, monkeypatch):
    monkeypatch.setenv("VECTOR_HOST", "vector-aggregator")
    log = closing_logger("test_vector_no_port")

    initialize_vector_logging(log, "x_did_finder")

    assert log.handlers == []


def test_initialize_vector_logging_adds_handler(vector_env, fake_sink, closing_logger):
    log = closing_logger("test_vector_adds")

    initialize_vector_logging(log, "x_did_finder")

    assert len(log.handlers) == 1
    assert isinstance(log.handlers[0], VectorQueueHandler)


def test_initialize_vector_logging_is_idempotent(vector_env, fake_sink, closing_logger):
    """initialize_logging() is reached from several modules; a second handler
    would double every row in log_messages."""
    log = closing_logger("test_vector_idempotent")

    initialize_vector_logging(log, "x_did_finder")
    initialize_vector_logging(log, "x_did_finder")

    assert len([h for h in log.handlers if isinstance(h, VectorQueueHandler)]) == 1


def test_initialize_vector_logging_records_reach_the_sink(
    vector_env, fake_sink, closing_logger
):
    log = closing_logger("test_vector_end_to_end")
    initialize_vector_logging(log, "rucio_did_finder")
    handler = log.handlers[0]

    log.info("Lookup finished", extra={"dataset_id": 7})
    handler.close()  # stop() drains the queue before returning

    assert len(fake_sink.instances) == 1
