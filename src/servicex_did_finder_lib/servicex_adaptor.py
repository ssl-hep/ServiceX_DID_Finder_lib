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
from datetime import datetime
import requests
import logging


MAX_RETRIES = 3


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
