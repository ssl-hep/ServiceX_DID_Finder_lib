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
"""
Ship DID finder log records to the release's Vector aggregator, which writes
them into the Postgres `log_messages` table.

A DID finder is a long-running deployment rather than a per-request pod, so it
talks to the aggregator's TCP socket source directly, exactly as the servicex
app does. There is no per-pod Vector sidecar to forward through.

Rows are keyed on `dataset_id`; a DID lookup has no request id.
"""
import logging
import os
import queue
import threading
from logging.handlers import QueueHandler, QueueListener

import logstash

instance = os.environ.get("INSTANCE_NAME", "Unknown")


class VectorFormatter(logstash.formatter.LogstashFormatterBase):
    """
    Serialize a log record to JSON whose keys are exactly the columns of the
    Postgres `log_messages` table. Any non-standard record attribute is nested
    under `extra` (a JSON column). Emits bytes, so it plugs straight into
    logstash.TCPLogstashHandler's newline framing.

    The aggregator does no field mapping -- it feeds the decoded object to
    `json_populate_recordset` -- so a key with no matching column fails the
    insert. Missing keys are fine and land as NULL, which is why `request_id`
    is absent here: a DID lookup belongs to a dataset, not to a request. This
    is the mirror image of the transformer sidecar's formatter, which omits
    `dataset_id` for the same reason.

    Unlike the app's and the sidecar's copies, `component` varies per finder
    (rucio_did_finder, cernopendata_did_finder, ...), so it is a constructor
    argument, matching LogstashFormatter in logstash_logging.py.
    """

    def __init__(self, component_name=None, message_type="Logstash", tags=None, fqdn=False):
        super().__init__(message_type, tags, fqdn)
        self.component_name = component_name

    def format(self, record):
        extra = self.get_extra_fields(record)
        message = {
            "timestamp": self.format_timestamp(record.created),
            "level": record.levelname,
            "logger": record.name,
            "instance": instance,
            "component": self.component_name,
            "message": record.getMessage(),
            "dataset_id": extra.pop("dataset_id", None),
            "extra": extra,
        }

        # If exception, add debug info
        if record.exc_info:
            message["extra"].update(self.get_debug_fields(record))

        return self.serialize(message)


class VectorQueueHandler(QueueHandler):
    """
    A QueueHandler whose listener thread and socket are rebuilt whenever the
    owning process changes.

    Logging is initialized at import, in the celery worker's MainProcess, but
    every DID finder runs the default prefork pool, so the tasks that do the
    logging run in forked children. Two things break without this guard:

      - A QueueListener is a thread, and threads do not survive fork(). Each
        child would inherit a QueueHandler feeding a queue that nobody drains:
        no rows in log_messages, and a queue that grows without bound.
      - TCPLogstashHandler connects lazily, and initialize_logging() logs a
        line of its own, so the socket is already open at fork time. Children
        sharing one fd interleave bytes mid-line and the aggregator's json
        decoder drops the result.

    Rebuilding on the first emit in a new process covers the prefork children,
    and also direct_test, unit tests, and any future change of --pool.
    """

    def __init__(self, host, port, component_name, level):
        super().__init__(queue.Queue(-1))
        self._host = host
        self._port = int(port)
        self._component_name = component_name
        self._level = level
        self._listener = None
        self._pid = None
        self._lock = threading.Lock()

    def _build_sink(self):
        sink = logstash.TCPLogstashHandler(self._host, self._port, version=1)
        sink.setFormatter(VectorFormatter(component_name=self._component_name))
        sink.setLevel(self._level)
        return sink

    def _ensure_listener(self):
        if self._pid == os.getpid():
            return
        with self._lock:
            if self._pid == os.getpid():
                return
            self._discard_inherited_listener()
            # A fresh queue as well as a fresh thread: whatever the parent left
            # in the old one belongs to the parent's listener, not to ours.
            self.queue = queue.Queue(-1)
            self._listener = QueueListener(
                self.queue, self._build_sink(), respect_handler_level=True
            )
            self._listener.start()
            self._pid = os.getpid()

    def _discard_inherited_listener(self):
        """
        Drop the listener we inherited across a fork rather than stop()ing it.
        Its thread object is dead in this process, so stop() would enqueue a
        sentinel nobody reads and then join() a thread that never ran. Closing
        our duplicate of its socket is refcounted, so the parent's copy stays
        open.
        """
        if self._listener is None:
            return
        for handler in self._listener.handlers:
            sock = getattr(handler, "sock", None)
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
                handler.sock = None
        self._listener = None

    def emit(self, record):
        # emit() rather than enqueue(): QueueHandler.emit calls prepare() and
        # then enqueue(), and prepare() does not touch self.queue, so swapping
        # the queue here is safe.
        self._ensure_listener()
        super().emit(record)

    def close(self):
        listener, self._listener = self._listener, None
        # Only stop a listener whose thread is actually running here. One
        # inherited from a parent process has no live thread to join.
        if listener is not None and self._pid == os.getpid():
            listener.stop()
        self._pid = None
        super().close()


def initialize_vector_logging(log, component_name):
    """
    Attach a Vector handler to `log`:

        log -> VectorQueueHandler (unbounded queue, non-blocking put)
            -> listener thread -> TCPLogstashHandler (newline-delimited JSON)
            -> the release's vector aggregator -> postgres log_messages.

    A no-op unless both VECTOR_HOST and VECTOR_PORT are set, so nothing changes
    when the chart's logging.vector.enabled is off.
    """
    host = os.environ.get("VECTOR_HOST")
    port = os.environ.get("VECTOR_PORT")
    if not (host and port):
        return

    # initialize_logging() is reached from several modules, so guard against a
    # second handler, which would double every row. Test by type rather than by
    # queue identity: VectorQueueHandler.queue legitimately changes identity
    # across a fork.
    if any(isinstance(handler, VectorQueueHandler) for handler in log.handlers):
        return

    level = log.level or logging.INFO
    handler = VectorQueueHandler(host, port, component_name, level)
    handler.setLevel(level)
    log.addHandler(handler)
