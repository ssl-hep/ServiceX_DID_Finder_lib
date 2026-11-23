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
"""Temporal entrypoint for DID finders.

DIDFinderWorker hosts the ``lookup_dataset`` activity that the ServiceX orchestrator's
DatasetLookupWorkflow schedules on the ``did-finder-<scheme>`` task queue. It wraps the
same ``find_files`` generator that DIDFinderApp (Celery) uses, so a finder only changes
its entrypoint.
"""
import asyncio
import logging
import os
import signal
import time
from datetime import timedelta
from typing import Any, Dict, Iterator, List, Optional

from temporalio import activity
from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

from servicex_did_finder_lib.did_finder_app import UserDIDHandler
from servicex_did_finder_lib.did_logging import initialize_root_logger
from servicex_did_finder_lib.did_summary import DIDSummary
from servicex_did_finder_lib.exceptions import to_application_error
from servicex_did_finder_lib.servicex_adaptor import ServiceXAdapter
from servicex_did_finder_lib.temporal_models import (
    LOOKUP_ACTIVITY_NAME,
    PROGRESS_SIGNAL,
    LookupInput,
    LookupSummary,
)
from servicex_did_finder_lib.util_uri import parse_did_uri

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 300
DEFAULT_FLUSH_INTERVAL_SECONDS = 5.0
# The orchestrator's heartbeat_timeout is 2 minutes; stay well inside it while the
# finder's generator is blocked on a slow catalog call.
KEEPALIVE_HEARTBEAT_SECONDS = 30.0
DEFAULT_MAX_CONCURRENT_LOOKUPS = 4
MAX_CONNECT_BACKOFF_SECONDS = 30.0

_DONE = object()


def strip_scheme(did: str, scheme: str) -> str:
    """The orchestrator passes the full DID (``rucio://scope:name``); finders expect
    it without the scheme, as the Celery path delivers it."""
    prefix = f"{scheme}://"
    return did[len(prefix):] if did.startswith(prefix) else did


def normalize_files(file_info: Any) -> List[Dict[str, Any]]:
    """A finder may yield one file dict or a list of them. Give each a file_key, the
    stable identity ServiceX dedupes on; default it to the first replica path."""
    if isinstance(file_info, dict):
        files = [file_info]
    elif isinstance(file_info, list):
        files = file_info
    else:
        raise ValueError("Invalid input: expected a dictionary or a list of dictionaries")
    for f in files:
        if not f.get('file_key'):
            f['file_key'] = f['paths'][0]
    return files


class LookupActivity:
    """The ``lookup_dataset`` activity bound to one finder's generator."""

    def __init__(self, user_did_finder: UserDIDHandler,
                 did_finder_args: Optional[Dict[str, Any]],
                 servicex_endpoint: str,
                 batch_size: int = DEFAULT_BATCH_SIZE,
                 flush_interval: float = DEFAULT_FLUSH_INTERVAL_SECONDS,
                 keepalive_interval: float = KEEPALIVE_HEARTBEAT_SECONDS):
        self.user_did_finder = user_did_finder
        self.did_finder_args = did_finder_args
        self.servicex_endpoint = servicex_endpoint
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.keepalive_interval = keepalive_interval

    @activity.defn(name=LOOKUP_ACTIVITY_NAME)
    async def lookup_dataset(self, input: LookupInput) -> LookupSummary:
        """Run the finder's generator and add its files to the dataset in batches.

        After each batch is committed the activity heartbeats
        ``{files_written, last_file_key}`` and signals ``progress(hwm)`` to the
        DatasetLookupWorkflow, where hwm is the highest files.id committed so far.
        Errors are raised as ApplicationErrors typed for the workflow's retry policy.
        """
        return await _Lookup(self, input).run()


class _Lookup:
    """State for one execution of lookup_dataset."""

    def __init__(self, owner: LookupActivity, input: LookupInput):
        self.owner = owner
        self.input = input
        self.log_extra = {"dataset_id": input.dataset_id}
        self.servicex = ServiceXAdapter(endpoint=owner.servicex_endpoint,
                                        dataset_id=input.dataset_id)
        self.summary = DIDSummary(input.did)
        self.hwm = 0
        self.last_file_key: Optional[str] = None
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush = time.monotonic()

    def heartbeat_details(self) -> Dict[str, Any]:
        return {"files_written": self.summary.file_count,
                "last_file_key": self.last_file_key}

    async def run(self) -> LookupSummary:
        info = activity.info()
        did_info = parse_did_uri(strip_scheme(self.input.did, self.input.did_scheme))
        resume = info.heartbeat_details[0] if info.heartbeat_details else None
        logger.info(f"Received DID request {did_info.did} (attempt {info.attempt})",
                    extra=self.log_extra)
        finder_info = {
            "dataset-id": self.input.dataset_id,
            "did-scheme": self.input.did_scheme,
            "did-hash": self.input.did_hash,
            # Details of the last committed batch when this is a retry. Finders that
            # list in a stable order may use last_file_key to skip ahead.
            "resume": resume,
        }

        start = time.monotonic()
        files: Optional[Iterator[Any]] = None
        keepalive = asyncio.create_task(self._keepalive())
        try:
            files = await asyncio.to_thread(
                lambda: iter(self.owner.user_did_finder(did_info.did, finder_info,
                                                        self.owner.did_finder_args)))
            while (file_info := await asyncio.to_thread(next, files, _DONE)) is not _DONE:
                self.buffer.extend(normalize_files(file_info))
                if did_info.file_count == -1 and self._should_flush():
                    await self._flush()

            if did_info.file_count > 0:
                # Sort so the same N files come back every time
                self.buffer = sorted(self.buffer, key=lambda f: f['paths'])
                self.buffer = self.buffer[:did_info.file_count]
            await self._flush()
        except asyncio.CancelledError:
            logger.info(f"Lookup of {did_info.did} cancelled", extra=self.log_extra)
            raise
        except Exception as e:
            logger.error(f"Error processing DID {did_info.did}", extra=self.log_extra,
                         exc_info=True)
            raise to_application_error(e) from e
        finally:
            keepalive.cancel()
            _close(files)

        logger.info(f"Lookup complete: {self.summary} in {time.monotonic() - start:.0f}s",
                    extra=self.log_extra)
        return LookupSummary(n_files=self.summary.file_count,
                             max_file_id=self.hwm,
                             size_bytes=self.summary.total_bytes,
                             events=self.summary.total_events)

    def _should_flush(self) -> bool:
        return (len(self.buffer) >= self.owner.batch_size
                or time.monotonic() - self.last_flush >= self.owner.flush_interval)

    async def _flush(self) -> None:
        self.last_flush = time.monotonic()
        if not self.buffer:
            return
        batch, self.buffer = self.buffer, []
        max_file_id = await asyncio.to_thread(self.servicex.put_files, batch)
        for f in batch:
            self.summary.add_file(f)
        self.hwm = max(self.hwm, max_file_id)
        self.last_file_key = batch[-1]['file_key']
        activity.heartbeat(self.heartbeat_details())
        handle = activity.client().get_workflow_handle(activity.info().workflow_id)
        await handle.signal(PROGRESS_SIGNAL, self.hwm)

    async def _keepalive(self) -> None:
        """Heartbeat while the finder's generator is blocked in its thread. This also
        lets a cancellation from the workflow reach the activity."""
        while True:
            await asyncio.sleep(self.owner.keepalive_interval)
            activity.heartbeat(self.heartbeat_details())


def _close(files: Optional[Iterator[Any]]) -> None:
    close = getattr(files, "close", None)
    if close is None:
        return
    try:
        close()
    except ValueError:
        # Still running in its thread after a cancellation; it is abandoned.
        pass


class DIDFinderWorker:
    """
    Temporal worker for a DID finder. Runs the ``lookup_dataset`` activity on the
    ``did-finder-<name>`` task queue. Usage:

        DIDFinderWorker("rucio", find_files, {"rucio_adapter": adapter}).run()

    ``find_files(did, info, did_finder_args)`` is the same generator DIDFinderApp uses.

    Every setting that is not passed falls back to an environment variable:
    TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_TASK_QUEUE,
    SERVICEX_INTERNAL_ENDPOINT and DID_FINDER_MAX_CONCURRENT.
    """

    def __init__(self, did_finder_name: str,
                 lookup: UserDIDHandler,
                 did_finder_args: Optional[Dict[str, Any]] = None,
                 *,
                 task_queue: Optional[str] = None,
                 temporal_address: Optional[str] = None,
                 temporal_namespace: Optional[str] = None,
                 servicex_endpoint: Optional[str] = None,
                 max_concurrent_lookups: Optional[int] = None,
                 batch_size: int = DEFAULT_BATCH_SIZE,
                 flush_interval: float = DEFAULT_FLUSH_INTERVAL_SECONDS):
        """
        Args:
            did_finder_name: The DID scheme this finder serves, e.g. "rucio".
            lookup: The user generator that yields file records for a DID.
            did_finder_args: Passed through to every call of ``lookup``.
            task_queue: Defaults to ``did-finder-<did_finder_name>``.
            temporal_address: Defaults to ``localhost:7233``.
            temporal_namespace: Defaults to ``default``.
            servicex_endpoint: The app's internal transformation endpoint, e.g.
                ``http://servicex-app:8000/servicex/internal/transformation/``. Required.
            max_concurrent_lookups: Lookups run at once by this worker. Defaults to 4.
            batch_size: Files sent to ServiceX per batch.
            flush_interval: Seconds after which a partial batch is sent.
        """
        self.name = did_finder_name
        initialize_root_logger(self.name)

        env = os.environ
        self.task_queue = (task_queue or env.get("TEMPORAL_TASK_QUEUE")
                           or f"did-finder-{did_finder_name}")
        self.temporal_address = (temporal_address or env.get("TEMPORAL_ADDRESS")
                                 or "localhost:7233")
        self.temporal_namespace = (temporal_namespace or env.get("TEMPORAL_NAMESPACE")
                                   or "default")
        self.max_concurrent_lookups = (max_concurrent_lookups
                                       or int(env.get("DID_FINDER_MAX_CONCURRENT",
                                                      DEFAULT_MAX_CONCURRENT_LOOKUPS)))
        endpoint = servicex_endpoint or env.get("SERVICEX_INTERNAL_ENDPOINT")
        if not endpoint:
            raise ValueError("No ServiceX endpoint: pass servicex_endpoint or set "
                             "SERVICEX_INTERNAL_ENDPOINT")
        self.servicex_endpoint = endpoint if endpoint.endswith("/") else endpoint + "/"

        self.activity = LookupActivity(lookup, did_finder_args, self.servicex_endpoint,
                                       batch_size=batch_size, flush_interval=flush_interval)

    async def connect(self) -> Client:
        """Connect to Temporal, waiting until the frontend and namespace exist so the
        pod does not crash-loop while Temporal is still coming up."""
        backoff = 1.0
        while True:
            try:
                client = await Client.connect(self.temporal_address,
                                              namespace=self.temporal_namespace)
                await client.workflow_service.describe_namespace(
                    DescribeNamespaceRequest(namespace=self.temporal_namespace))
                return client
            except RuntimeError as err:
                # Client.connect raises RuntimeError when the frontend is unreachable
                logger.warning(f"Temporal frontend {self.temporal_address} "
                               f"not reachable yet: {err}")
            except RPCError as err:
                if err.status == RPCStatusCode.NOT_FOUND:
                    logger.warning(f"Temporal namespace {self.temporal_namespace!r} "
                                   f"does not exist yet")
                elif err.status == RPCStatusCode.UNAVAILABLE:
                    logger.warning(f"Temporal frontend unavailable: {err}")
                else:
                    raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_CONNECT_BACKOFF_SECONDS)

    def build_worker(self, client: Client) -> Worker:
        return Worker(
            client,
            task_queue=self.task_queue,
            activities=[self.activity.lookup_dataset],
            max_concurrent_activities=self.max_concurrent_lookups,
            graceful_shutdown_timeout=timedelta(seconds=20),
        )

    async def run_async(self, stop: Optional[asyncio.Event] = None) -> None:
        """Poll the task queue until ``stop`` is set (or SIGTERM/SIGINT arrives)."""
        if stop is None:
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, stop.set)

        connecting = asyncio.create_task(self.connect())
        stopping = asyncio.create_task(stop.wait())
        await asyncio.wait({connecting, stopping}, return_when=asyncio.FIRST_COMPLETED)
        if not connecting.done():
            connecting.cancel()
            logger.info("Shutdown requested before Temporal was reachable")
            return
        stopping.cancel()
        client = connecting.result()

        logger.info(f"DID finder {self.name} polling task queue {self.task_queue!r} "
                    f"in namespace {self.temporal_namespace!r} at {self.temporal_address}")
        async with self.build_worker(client):
            await stop.wait()
            logger.info("Shutdown requested, draining in-flight lookups")

    def run(self) -> None:
        asyncio.run(self.run_async())
