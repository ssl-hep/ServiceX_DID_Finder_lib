# Copyright (c) 2026, IRIS-HEP
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
import asyncio
import dataclasses
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from servicex_did_finder_lib import exceptions
from servicex_did_finder_lib.did_finder_worker import DIDFinderWorker, LookupActivity
from servicex_did_finder_lib.servicex_adaptor import ServiceXAdapter
from servicex_did_finder_lib.temporal_models import LookupInput, LookupSummary

ENDPOINT = "http://servicex.org/servicex/internal/transformation/"


def make_file(i, **extra):
    return dict({"paths": [f"root://site/f{i:03}.root", f"root://other/f{i:03}.root"],
                 "adler32": "deadbeef", "file_size": 10, "file_events": 2}, **extra)


@pytest.fixture()
def sent(monkeypatch):
    """Patch ServiceXAdapter.put_files to record each batch and hand out sequential ids"""
    batches = []

    def put_files(self, file_list, chunk_length=300):
        batches.append([dict(f) for f in file_list])
        return sum(len(b) for b in batches) + 100  # ids start at 101

    monkeypatch.setattr(ServiceXAdapter, "put_files", put_files)
    return batches


@pytest.fixture()
def handle():
    return MagicMock(signal=AsyncMock())


@pytest.fixture()
def env(handle):
    client = MagicMock()
    client.get_workflow_handle.return_value = handle
    env = ActivityEnvironment(client=client)
    env.mock_client = client
    env.heartbeats = []
    env.on_heartbeat = lambda *details: env.heartbeats.append(details[0])
    return env


def run_lookup(env, finder, did="rucio://scope:ds", args=None, **kwargs):
    act = LookupActivity(finder, args, ENDPOINT, **kwargs)
    lookup = LookupInput(did_hash="abc", did=did, did_scheme="rucio", dataset_id=42)
    return asyncio.run(env.run(act.lookup_dataset, lookup))


def test_lookup_sends_batches_and_signals_progress(env, handle, sent):
    calls = []

    def finder(did, info, args):
        calls.append((did, info, args))
        for i in range(5):
            yield make_file(i)

    summary = run_lookup(env, finder, args={"x": 1}, batch_size=2, flush_interval=3600)

    assert summary == LookupSummary(n_files=5, max_file_id=105, size_bytes=50, events=10)
    assert [len(b) for b in sent] == [2, 2, 1]
    assert [c.args for c in handle.signal.await_args_list] == [
        ("progress", 102), ("progress", 104), ("progress", 105)]
    assert env.heartbeats[-1] == {"files_written": 5,
                                  "last_file_key": "root://site/f004.root"}

    did, info, args = calls[0]
    assert did == "scope:ds"  # scheme stripped
    assert info == {"dataset-id": 42, "did-scheme": "rucio", "did-hash": "abc",
                    "resume": None}
    assert args == {"x": 1}


def test_lookup_signals_the_calling_workflow(env, sent):
    env.info = dataclasses.replace(env.info, workflow_id="dataset-abc")
    run_lookup(env, lambda did, info, args: iter([make_file(0)]))
    env.mock_client.get_workflow_handle.assert_called_with("dataset-abc")


def test_lookup_flushes_on_interval(env, sent):
    def finder(did, info, args):
        for i in range(3):
            yield make_file(i)

    run_lookup(env, finder, batch_size=100, flush_interval=0)
    assert [len(b) for b in sent] == [1, 1, 1]


def test_lookup_accepts_lists_and_keeps_file_key(env, sent):
    def finder(did, info, args):
        yield [make_file(0), make_file(1, file_key="scope:f001")]

    summary = run_lookup(env, finder)
    assert summary.n_files == 2
    assert [f["file_key"] for f in sent[0]] == ["root://site/f000.root", "scope:f001"]


def test_lookup_file_limit_sorts_and_truncates(env, sent):
    def finder(did, info, args):
        assert did == "scope:ds?other=1"
        for i in [4, 2, 3, 0, 1]:
            yield make_file(i)

    summary = run_lookup(env, finder, did="rucio://scope:ds?files=2&other=1", batch_size=1)
    assert len(sent) == 1  # nothing sent until the generator finished
    first_paths = [f["paths"][0] for f in sent[0]]
    assert first_paths == ["root://site/f000.root", "root://site/f001.root"]
    assert summary.n_files == 2


def test_lookup_empty_dataset(env, handle, sent):
    summary = run_lookup(env, lambda did, info, args: iter([]))
    assert summary == LookupSummary(n_files=0, max_file_id=0, size_bytes=0, events=0)
    assert sent == []
    handle.signal.assert_not_awaited()


def test_lookup_passes_resume_details_on_retry(env, sent):
    seen = []
    details = {"files_written": 300, "last_file_key": "scope:f299"}
    env.info = dataclasses.replace(env.info, attempt=2, heartbeat_details=[details])

    def finder(did, info, args):
        seen.append(info["resume"])
        yield make_file(0)

    run_lookup(env, finder)
    assert seen == [details]


def test_lookup_heartbeats_while_finder_blocks(env, sent):
    def finder(did, info, args):
        time.sleep(0.3)
        yield make_file(0)

    run_lookup(env, finder, keepalive_interval=0.05)
    assert env.heartbeats.count({"files_written": 0, "last_file_key": None}) >= 2


def test_lookup_closes_generator(env, sent):
    closed = []

    def finder(did, info, args):
        try:
            yield make_file(0)
            raise exceptions.LookupFailureException("boom")
        finally:
            closed.append(True)

    with pytest.raises(ApplicationError):
        run_lookup(env, finder)
    assert closed == [True]


@pytest.mark.parametrize("exc, error_type, non_retryable", [
    (exceptions.NoSuchDatasetException("gone"), "DatasetNotFound", True),
    (exceptions.BadDatasetNameException("bad"), "BadDID", True),
    (exceptions.AuthFailedException("x509"), "AuthFailed", True),
    (exceptions.LookupFailureException("rucio down"), "LookupFailed", False),
    (RuntimeError("bug"), "LookupFailed", False),
])
def test_lookup_errors_become_application_errors(env, sent, exc, error_type, non_retryable):
    def finder(did, info, args):
        raise exc
        yield  # pragma: no cover

    with pytest.raises(ApplicationError) as err:
        run_lookup(env, finder)
    assert err.value.type == error_type
    assert err.value.non_retryable is non_retryable
    assert err.value.__cause__ is exc


def test_lookup_servicex_failure_is_retryable(env, monkeypatch):
    def put_files(self, file_list, chunk_length=300):
        raise ConnectionError("app down")

    monkeypatch.setattr(ServiceXAdapter, "put_files", put_files)
    with pytest.raises(ApplicationError) as err:
        run_lookup(env, lambda did, info, args: iter([make_file(0)]))
    assert err.value.type == "LookupFailed"
    assert not err.value.non_retryable


def test_lookup_bad_yield_is_an_error(env, sent):
    with pytest.raises(ApplicationError) as err:
        run_lookup(env, lambda did, info, args: iter(["not a file"]))
    assert err.value.type == "LookupFailed"


def test_worker_defaults_from_env(monkeypatch):
    monkeypatch.setenv("TEMPORAL_ADDRESS", "temporal:7233")
    monkeypatch.setenv("TEMPORAL_NAMESPACE", "servicex")
    monkeypatch.setenv("SERVICEX_INTERNAL_ENDPOINT", "http://app:8000/internal")
    monkeypatch.setenv("DID_FINDER_MAX_CONCURRENT", "7")
    monkeypatch.delenv("TEMPORAL_TASK_QUEUE", raising=False)

    worker = DIDFinderWorker("rucio", lambda did, info, args: iter([]))
    assert worker.task_queue == "did-finder-rucio"
    assert worker.temporal_address == "temporal:7233"
    assert worker.temporal_namespace == "servicex"
    assert worker.servicex_endpoint == "http://app:8000/internal/"
    assert worker.max_concurrent_lookups == 7


def test_worker_arguments_override_env(monkeypatch):
    monkeypatch.setenv("TEMPORAL_TASK_QUEUE", "from-env")
    worker = DIDFinderWorker("rucio", lambda did, info, args: iter([]),
                             task_queue="q", temporal_address="t:1", temporal_namespace="n",
                             servicex_endpoint="http://app/", max_concurrent_lookups=2)
    assert (worker.task_queue, worker.temporal_address, worker.temporal_namespace,
            worker.servicex_endpoint, worker.max_concurrent_lookups) == \
        ("q", "t:1", "n", "http://app/", 2)


def test_worker_requires_endpoint(monkeypatch):
    monkeypatch.delenv("SERVICEX_INTERNAL_ENDPOINT", raising=False)
    with pytest.raises(ValueError):
        DIDFinderWorker("rucio", lambda did, info, args: iter([]))


def test_worker_stops_before_temporal_is_reachable():
    worker = DIDFinderWorker("rucio", lambda did, info, args: iter([]),
                             temporal_address="127.0.0.1:1", servicex_endpoint="http://app/")
    stop = asyncio.Event()
    stop.set()
    asyncio.run(asyncio.wait_for(worker.run_async(stop), timeout=10))
