# Copyright (c) 2021-2025, IRIS-HEP
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

import argparse
from datetime import datetime
import json
import logging
import time
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Union
import sys

import pika
from make_it_sync import make_sync

from servicex_did_finder_lib.did_summary import DIDSummary
from servicex_did_finder_lib.did_logging import initialize_root_logger
from servicex_did_finder_lib.util_uri import parse_did_uri
from .servicex_adaptor import ServiceXAdapter

# The type for the callback method to handle DID's, supplied by the user.
UserDIDHandler = Callable[[str, Dict[str, Any]], AsyncGenerator[Union[Dict[str, Any],
                                                                      List[Dict[str, Any]]], None]]

# Given name, build the RabbitMQ queue name by appending this.
# This is backed into how ServiceX works - do not change unless it
# is also changed in the ServiceX_App
QUEUE_NAME_POSTFIX = "_did_requests"

# Easy to use local logger
__logging = logging.getLogger(__name__)
__logging.addHandler(logging.NullHandler())

# TODO: get rid of different modes. It should always be batch.


class _accumulator:
    "Track or cache files depending on the mode we are operating in"

    def __init__(self, sx: ServiceXAdapter, sum: DIDSummary, hold_till_end: bool):
        self._servicex = sx
        self._summary = sum
        self._hold_till_end = hold_till_end
        self._file_cache: List[Dict[str, Any]] = []

    def add(self, file_info: Dict[str, Any]):
        "Track and inject the file back into the system"
        if self._hold_till_end:
            self._file_cache.append(file_info)
        else:
            self._summary.add_file(file_info)
            self._servicex.put_file_add(file_info)

    def send_on(self, count):
        "Send the accumulated files"
        if self._hold_till_end:
            self._hold_till_end = False
            files = sorted(self._file_cache, key=lambda x: x["paths"])
            if count == -1:
                self.send_bulk(files)
            else:
                self.send_bulk(files[:count])

    def send_bulk(self, file_list: List[Dict[str, Any]]):
        "does a bulk put of files"
        if self._hold_till_end:
            for f in file_list:
                self.add(f)
        else:
            for ifl in file_list:
                self._summary.add_file(ifl)
            self._servicex.put_file_add_bulk(file_list)


async def run_file_fetch_loop(
    did: str,
    servicex: ServiceXAdapter,
    info: Dict[str, Any],
    user_callback: UserDIDHandler,
):
    start_time = datetime.now()

    summary = DIDSummary(did)
    did_info = parse_did_uri(did)
    hold_till_end = did_info.file_count != -1
    acc = _accumulator(servicex, summary, hold_till_end)

    try:
        async for file_info in user_callback(did_info.did, info):
            if type(file_info) is dict:
                acc.add(file_info)
            else:
                acc.send_bulk(file_info)

    except Exception:
        if did_info.get_mode == "all":
            raise

    # If we've been holding onto any files, we need to send them now.
    acc.send_on(did_info.file_count)

    elapsed_time = int((datetime.now() - start_time).total_seconds())
    servicex.put_fileset_complete(
        {
            "files": summary.file_count,
            "files-skipped": summary.files_skipped,
            "total-events": summary.total_events,
            "total-bytes": summary.total_bytes,
            "elapsed-time": elapsed_time,
        }
    )


def rabbit_mq_callback(
    user_callback: UserDIDHandler, channel, method, properties, body
):
    """rabbit_mq_callback Respond to RabbitMQ Message

    When a request to resolve a DID comes into the DID finder, we
    respond with this callback. This callback will remain active
    until the request has been completed satisfied (so for some
    DID finders, this could be a fairly long time.)

    Args:
        channel ([type]): RabbitMQ channel
        method ([type]): Delivery method
        properties ([type]): Properties of the message
        body ([type]): The body (json for us) of the message
    """
    dataset_id = None  # set this in case we get an exception while loading request
    try:
        # Unpack the message. Really bad if we fail up here!
        did_request = json.loads(body)
        did = did_request["did"]
        dataset_id = did_request["dataset_id"]
        endpoint = did_request["endpoint"]
        __logging.info(
            f"Received DID request {did_request}",
            extra={"dataset_id": dataset_id}
        )
        servicex = ServiceXAdapter(dataset_id=dataset_id,
                                   endpoint=endpoint)

        info = {
            "dataset-id": dataset_id,
        }

        # Process the request and resolve the DID
        try:
            make_sync(run_file_fetch_loop)(did, servicex, info, user_callback)

        except Exception as e:
            _, exec_value, _ = sys.exc_info()
            __logging.exception(f"DID Request Failed {str(e)}", extra={"dataset_id": dataset_id})
            raise

    except Exception as e:
        __logging.exception(
            f"DID request failed {str(e)}", extra={"dataset_id": dataset_id}
        )

    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)


def init_rabbit_mq(
    user_callback: UserDIDHandler,
    rabbitmq_url: str,
    queue_name: str,
    retries: int,
    retry_interval: float,
):  # type: ignore
    rabbitmq = None
    retry_count = 0

    while not rabbitmq:
        try:
            rabbitmq = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
            _channel = rabbitmq.channel()
            _channel.queue_declare(queue=queue_name)

            __logging.info("Connected to RabbitMQ. Ready to start consuming requests")

            _channel.basic_consume(
                queue=queue_name,
                auto_ack=False,
                on_message_callback=lambda c, m, p, b: rabbit_mq_callback(
                    user_callback, c, m, p, b
                ),
            )
            _channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:  # type: ignore
            rabbitmq = None
            retry_count += 1
            if retry_count <= retries:
                __logging.exception(
                    f"Failed to connect to RabbitMQ at {rabbitmq_url} "
                    f"(try #{retry_count}). Waiting {retry_interval} seconds "
                    "before trying again"
                )
                time.sleep(retry_interval)
            else:
                __logging.exception(
                    f"Failed to connect to RabbitMQ. Giving Up after {retry_count}"
                    " tries"
                )
                raise


def add_did_finder_cnd_arguments(parser: argparse.ArgumentParser):
    """add_did_finder_cnd_arguments Add required arguments to a parser

    If you need to parse command line arguments for some special configuration, create your
    own argument parser, and call this function to make sure the arguments needed
    for running the back-end communication are filled in properly.

    Then pass the results of the parsing to the `start_did_finder` method.

    Args:
        parser (argparse.ArgumentParser): The argument parser. Arguments needed for the
                                          did finder/servicex communication will be added.
    """
    parser.add_argument(
        "--rabbit-uri", dest="rabbit_uri", action="store", required=True
    )
    parser.add_argument(
        "--prefix",
        dest="prefix",
        action="store",
        required=False,
        default="",
        help="Prefix to add to use a caching proxy for URIs",
    )


def start_did_finder(
    did_finder_name: str,
    callback: UserDIDHandler,
    parsed_args: Optional[argparse.Namespace] = None,
):
    """start_did_finder Start the DID finder

    Top level method that starts the DID finder, hooking it up to rabbitmq queues, etc.,
    and sets up the callback to be called each time ServiceX wants to render a DID into
    files.

    Once called this will not return unless it totally fails to connect to the rabbit
    mq server (which must have been specified on the command line that started this).
    If it can't return, then it will raise the appropriate exception.

    Args:
        did_name (str): Name of the DID finder (baked into the rabbit mq name)
        callback (UserDIDHandler): Callback to handle the DID rendering requests
        parser (Optional[argparse.Namespace], optional): If you need to parse your own
                                                         an arg parser, make sure to call
                                                         add_did_finder_cnd_arguments, run the
                                                         parser and pass in the resulting
                                                         parsed arguments.
                                                         Defaults to None (automatically
                                                         parses)
                                                         command line arguments, create
    """
    # Setup command line parsing
    if parsed_args is None:
        parser = argparse.ArgumentParser()
        add_did_finder_cnd_arguments(parser)
        parsed_args = parser.parse_args()

    # Initialize the root logger
    initialize_root_logger(did_finder_name)

    # Start up rabbit mq and also callbacks
    init_rabbit_mq(
        callback,
        parsed_args.rabbit_uri,
        f"{did_finder_name}{QUEUE_NAME_POSTFIX}",
        retries=12,
        retry_interval=10,
    )
