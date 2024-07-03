# Copyright (c) 2024, IRIS-HEP
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
import logging
from typing import Any, Dict, Generator

from servicex_did_finder_lib import DIDFinderApp

__log = logging.getLogger(__name__)


def find_files(did_name: str,
               info: Dict[str, Any],
               did_finder_args: dict = None) -> Generator[Dict[str, Any], None, None]:
    for i in range(int(did_finder_args['num_files'])):
        yield {
            'paths': did_finder_args['file_path'],
            'adler32': 0,  # No clue
            'file_size': 0,  # Size in bytes if known
            'file_events': i,  # Include clue of how far we've come
        }


def run_open_data():
    # Parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-files', dest='num_files', action='store',
                        default='10',
                        help='Number of files to generate for each dataset')

    parser.add_argument('--file-path', dest='file_path', action='store',
                        default='',
                        help='Path to a file to be returned in each response')

    DIDFinderApp.add_did_finder_cnd_arguments(parser)

    __log.info('Starting Stressful DID finder')
    app = DIDFinderApp('stressful_did_finder', parsed_args=parser.parse_args())

    @app.did_lookup_task(name="stressful_did_finder.lookup_dataset")
    def lookup_dataset(self, did: str, dataset_id: int, endpoint: str) -> None:
        self.do_lookup(did=did, dataset_id=dataset_id,
                       endpoint=endpoint, user_did_finder=find_files)

    app.start()


if __name__ == "__main__":
    run_open_data()
