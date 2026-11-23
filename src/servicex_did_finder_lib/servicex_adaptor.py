# Copyright (c) 2019-2025, IRIS-HEP
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
import time
from datetime import datetime
import requests
import logging


MAX_RETRIES = 3

# Seconds to wait on the ServiceX app for the Temporal path's requests
REQUEST_TIMEOUT = 60
# Seconds to wait for a connection to the app. Short, so the quick retries below
# stay quick when the app is unreachable.
CONNECT_TIMEOUT = 5

# put_files retries a connection failure or one of these statuses after each of
# these delays, then raises. That rides out a blip (an app pod restarting) without
# failing the lookup activity, whose retry re-runs the whole catalog query. Anything
# longer is left to the orchestrator's retry policy.
PUT_FILES_RETRY_DELAYS = (1.0, 3.0)
PUT_FILES_RETRY_STATUSES = frozenset({502, 503, 504})


class ServiceXAdapter:
    def __init__(self, endpoint, dataset_id):
        self.endpoint = endpoint
        self.dataset_id = dataset_id

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())

    def _create_json(self, file_info):
        return {
            "timestamp": datetime.now().isoformat(),
            "paths": file_info['paths'],
            'adler32': file_info['adler32'],
            'file_size': file_info['file_size'],
            'file_events': file_info['file_events']
        }

    def put_file_add_bulk(self, file_list, chunk_length=300):
        # we first chunk up file_list as it can be very large in
        # case there are a lot of replicas and a lot of files.
        chunks = [file_list[i:i + chunk_length] for i in range(0, len(file_list), chunk_length)]
        for chunk in chunks:
            success = False
            attempts = 0
            mesg = []
            for fi in chunk:
                mesg.append(self._create_json(fi))
            while not success and attempts < MAX_RETRIES:
                try:
                    requests.put(f"{self.endpoint}{self.dataset_id}/files", json=mesg)
                    self.logger.info(f"Metric: {json.dumps(mesg)}")
                    success = True
                except requests.exceptions.ConnectionError:
                    self.logger.exception(f'Connection error to ServiceX App. Will retry '
                                          f'(try {attempts} out of {MAX_RETRIES}')
                    attempts += 1
            if not success:
                self.logger.error(f'After {attempts} tries, failed to send ServiceX App '
                                  f'a put_file_bulk message: {mesg} - Ignoring error.')

    def put_files(self, file_list, chunk_length=300) -> int:
        """Add files to the dataset for the Temporal DIDFinderWorker.

        Unlike put_file_add_bulk this does not swallow errors. Each chunk gets a few
        quick retries for a connection failure or a 502/503/504; after that, and for
        any other HTTP error status, it raises so the Temporal activity fails and is
        retried by the orchestrator. Resending a chunk is safe: the app dedupes on
        file_key, which each file carries.

        Returns:
            The highest files.id the app reported committing, or 0 if none.
        """
        max_file_id = 0
        for i in range(0, len(file_list), chunk_length):
            mesg = [
                dict(self._create_json(fi), file_key=fi['file_key'])
                for fi in file_list[i:i + chunk_length]
            ]
            response = self._put_files_chunk(mesg)
            reported = response.json().get("max_file_id")
            if reported is None:
                raise ValueError("ServiceX did not report max_file_id when adding files. "
                                 "The DIDFinderWorker needs a newer ServiceX app.")
            max_file_id = max(max_file_id, int(reported))
        return max_file_id

    def _put_files_chunk(self, mesg) -> requests.Response:
        """PUT one chunk of files, retrying blips per PUT_FILES_RETRY_DELAYS."""
        url = f"{self.endpoint}{self.dataset_id}/files"
        attempt = 1
        while True:
            last_attempt = attempt > len(PUT_FILES_RETRY_DELAYS)
            try:
                response = requests.put(url, json=mesg,
                                        timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT))
            except requests.exceptions.ConnectionError as e:
                if last_attempt:
                    raise
                problem = f"{type(e).__name__}: {e}"
            else:
                if response.status_code not in PUT_FILES_RETRY_STATUSES or last_attempt:
                    response.raise_for_status()
                    return response
                problem = f"HTTP {response.status_code}"
            delay = PUT_FILES_RETRY_DELAYS[attempt - 1]
            self.logger.warning(
                f"Adding {len(mesg)} files to dataset {self.dataset_id} failed "
                f"(try {attempt} of {len(PUT_FILES_RETRY_DELAYS) + 1}, {problem}). "
                f"Retrying in {delay:g}s",
                extra={"dataset_id": self.dataset_id})
            time.sleep(delay)
            attempt += 1

    def put_file_add(self, file):
        # add one file
        self.put_file_add_bulk([file])

    def put_fileset_complete(self, summary):
        success = False
        attempts = 0
        while not success and attempts < MAX_RETRIES:
            try:
                requests.put(f"{self.endpoint}{self.dataset_id}/complete", json=summary)
                success = True
            except requests.exceptions.ConnectionError:
                self.logger.exception(f'Connection error to ServiceX App. Will retry '
                                      f'(try {attempts} out of {MAX_RETRIES}')
                attempts += 1
        if not success:
            self.logger.error(f'After {attempts} tries, failed to send ServiceX App a put_file '
                              f'message: {str(summary)} - Ignoring error.')

    def put_fileset_error(self, summary):
        success = False
        attempts = 0
        while not success and attempts < MAX_RETRIES:
            try:
                requests.put(f"{self.endpoint}{self.dataset_id}/error", json=summary)
                success = True
            except requests.exceptions.ConnectionError:
                self.logger.exception(f'Connection error to ServiceX App. Will retry '
                                      f'(try {attempts} out of {MAX_RETRIES}')
                attempts += 1
        if not success:
            self.logger.error(f'After {attempts} tries, failed to send ServiceX App a put_file '
                              f'message: {str(summary)} - Ignoring error.')
