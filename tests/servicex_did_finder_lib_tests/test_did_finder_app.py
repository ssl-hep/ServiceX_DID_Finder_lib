# Copyright (c) 2024-2025, IRIS-HEP
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
from unittest.mock import patch
import pytest
from celery import Celery

from servicex_did_finder_lib.accumulator import Accumulator
from servicex_did_finder_lib.did_finder_app import DIDFinderTask, DIDFinderApp
from servicex_did_finder_lib import exceptions


@pytest.fixture()
def servicex(mocker):
    """Return a ServiceXAdaptor for testing"""
    with patch(
        "servicex_did_finder_lib.did_finder_app.ServiceXAdapter", autospec=True
    ) as sx_ctor:
        sx_adaptor = mocker.MagicMock()
        sx_ctor.return_value = sx_adaptor

        yield sx_ctor


def test_did_finder_task(mocker, servicex, single_file_info):
    did_finder_task = DIDFinderTask()
    # did_finder_task.app = mocker.Mock()
    did_finder_task.app.did_finder_args = {}
    mock_generator = mocker.Mock(return_value=iter([single_file_info]))

    mock_accumulator = mocker.MagicMock(Accumulator)
    with patch(
        "servicex_did_finder_lib.did_finder_app.Accumulator", autospec=True
    ) as acc:
        acc.return_value = mock_accumulator
        did_finder_task.do_lookup('did', 1, 'https://my-servicex', mock_generator)
        servicex.assert_called_with(dataset_id=1, endpoint="https://my-servicex")
        acc.assert_called_once()

        mock_accumulator.add.assert_called_with(single_file_info)
        mock_accumulator.send_on.assert_called_with(-1)

        servicex.return_value.put_fileset_complete.assert_called_with(
            {
                "files": 0,  # Aught to have a side effect in mock accumulator that updates this
                "files-skipped": 0,
                "total-events": 0,
                "total-bytes": 0,
                "elapsed-time": 0,
            }
        )


@pytest.mark.parametrize("exc", [Exception("Boom"),
                                 exceptions.BadDatasetNameException("Bad name"),
                                 exceptions.LookupFailureException("Boom 2"),
                                 exceptions.NoSuchDatasetException("Not there")])
def test_did_finder_task_exception(mocker, servicex, exc, single_file_info):
    did_finder_task = DIDFinderTask()
    # did_finder_task.app = mocker.Mock()
    did_finder_task.app.did_finder_args = {}
    mock_generator = mocker.Mock(side_effect=exc)

    mock_accumulator = mocker.MagicMock(Accumulator)
    with patch(
        "servicex_did_finder_lib.did_finder_app.Accumulator", autospec=True
    ) as acc:
        acc.return_value = mock_accumulator
        did_finder_task.do_lookup('did', 1, 'https://my-servicex', mock_generator)
        servicex.assert_called_with(dataset_id=1, endpoint="https://my-servicex")
        acc.assert_called_once()

        mock_accumulator.add.assert_not_called()
        mock_accumulator.send_on.assert_not_called()

        error_type_str = (exc.error_type
                          if isinstance(exc, exceptions.BaseDIDFinderException)
                          else "internal_failure")
        servicex.return_value.put_fileset_error.assert_called_with(
            {
                "elapsed-time": 0,
                "error-type": error_type_str,
                "message": str(exc),
            }
        )


def test_celery_app():
    app = DIDFinderApp('foo')
    assert isinstance(app, Celery)
    assert app.name == 'foo'

    @app.did_lookup_task(name="did_finder_rucio.lookup_dataset")
    def lookup_dataset(self, did: str, dataset_id: int, endpoint: str) -> None:
        self.do_lookup(did=did, dataset_id=dataset_id,
                       endpoint=endpoint, user_did_finder=lambda x, y, z: None)

    assert lookup_dataset.__name__ == 'wrapper'
