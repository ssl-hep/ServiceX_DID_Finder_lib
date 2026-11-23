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
import json

import pytest
import requests
import responses
from servicex_did_finder_lib import servicex_adaptor
from servicex_did_finder_lib.servicex_adaptor import ServiceXAdapter


@responses.activate
def test_put_file_add_bulk():
    call_count = 0

    def request_callback(request):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise requests.exceptions.ConnectionError("Connection failed")
        else:
            return (206, {}, "")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/files',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_file_add_bulk([{
        'paths': ['root://foo.bar.ROOT'],
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    }, {
        'paths': ['root://foo.bar1.ROOT'],
        'adler32': '33',
        'file_size': 1025,
        'file_events': 3142
    }])

    assert len(responses.calls) == 1 + 1  # 1 retry
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted[0]['paths'][0] == 'root://foo.bar.ROOT'
    assert submitted[0]['adler32'] == '32'
    assert submitted[0]['file_events'] == 3141
    assert submitted[0]['file_size'] == 1024

    assert submitted[1]['paths'][0] == 'root://foo.bar1.ROOT'
    assert submitted[1]['adler32'] == '33'
    assert submitted[1]['file_events'] == 3142
    assert submitted[1]['file_size'] == 1025


@responses.activate
def test_put_file_add_bulk_large():

    responses.add(responses.PUT, 'http://servicex.org/12345/files', status=206)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_file_add_bulk([{
        'paths': ['root://foo.bar.ROOT'],
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    }] * 320)
    assert len(responses.calls) == 2  # No retries


@responses.activate
def test_put_file_add_bulk_failure():

    def request_callback(request):
        raise requests.exceptions.ConnectionError("Connection failed")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/files',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_file_add_bulk([{
        'paths': ['root://foo.bar.ROOT'],
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    }, {
        'paths': ['root://foo.bar1.ROOT'],
        'adler32': '33',
        'file_size': 1025,
        'file_events': 3142
    }])

    assert len(responses.calls) == 3  # Max retries


@responses.activate
def test_put_file_complete():
    call_count = 0

    def request_callback(request):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise requests.exceptions.ConnectionError("Connection failed")
        else:
            return (206, {}, "")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/complete',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_fileset_complete({
        "files": 100,
        "files-skipped": 0,
        "total-events": 1000,
        "total-bytes": 10000,
        "elapsed-time": 10
    })
    assert len(responses.calls) == 1 + 1  # 1 retry
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted['files'] == 100
    assert submitted['files-skipped'] == 0
    assert submitted['total-events'] == 1000
    assert submitted['total-bytes'] == 10000
    assert submitted['elapsed-time'] == 10


@responses.activate
def test_put_file_complete_failure():

    def request_callback(request):
        raise requests.exceptions.ConnectionError("Connection failed")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/complete',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_fileset_complete({
        "files": 100,
        "files-skipped": 0,
        "total-events": 1000,
        "total-bytes": 10000,
        "elapsed-time": 10
    })
    assert len(responses.calls) == 3  # Max retries


@responses.activate
def test_put_file_error():
    call_count = 0

    def request_callback(request):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise requests.exceptions.ConnectionError("Connection failed")
        else:
            return (206, {}, "")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/error',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_fileset_error({
        "error-type": "bad_name",
        "elapsed-time": 10
    })
    assert len(responses.calls) == 1 + 1  # 1 retry
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted['error-type'] == "bad_name"
    assert submitted['elapsed-time'] == 10


@responses.activate
def test_put_file_error_failure():

    def request_callback(request):
        raise requests.exceptions.ConnectionError("Connection failed")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/error',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_fileset_error({
        "error-type": "bad_name",
        "elapsed-time": 10
    })
    assert len(responses.calls) == 3  # Max retries


FILES_URL = 'http://servicex.org/12345/files'


def _files(n):
    return [{
        'paths': [f'root://foo/file{i}.ROOT'],
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141,
        'file_key': f'foo/file{i}',
    } for i in range(n)]


@pytest.fixture
def sleeps(monkeypatch):
    """Record put_files' retry waits instead of sleeping."""
    waits = []
    monkeypatch.setattr(servicex_adaptor.time, "sleep", waits.append)
    return waits


def _replies(*outcomes):
    """A responses callback giving each outcome in turn: an int status, or an
    exception to raise. A 200 reports max_file_id."""
    remaining = list(outcomes)

    def callback(request):
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        body = json.dumps({"max_file_id": 7}) if outcome == 200 else ""
        return (outcome, {}, body)
    return callback


@responses.activate
def test_put_files_reports_max_file_id(sleeps):
    responses.add_callback(responses.PUT, FILES_URL, callback=_replies(200))
    sx = ServiceXAdapter("http://servicex.org/", '12345')

    assert sx.put_files(_files(2)) == 7
    [call] = responses.calls
    sent = [f['file_key'] for f in json.loads(call.request.body)]
    assert sent == ['foo/file0', 'foo/file1']
    assert sleeps == []


@responses.activate
def test_put_files_retries_a_connection_blip(sleeps):
    responses.add_callback(responses.PUT, FILES_URL, callback=_replies(
        requests.exceptions.ConnectionError("refused"), 200))
    sx = ServiceXAdapter("http://servicex.org/", '12345')

    assert sx.put_files(_files(1)) == 7
    assert len(responses.calls) == 2
    assert sleeps == [1.0]


@responses.activate
def test_put_files_retries_unavailable(sleeps):
    responses.add_callback(responses.PUT, FILES_URL, callback=_replies(503, 502, 200))
    sx = ServiceXAdapter("http://servicex.org/", '12345')

    assert sx.put_files(_files(1)) == 7
    assert len(responses.calls) == 3
    assert sleeps == list(servicex_adaptor.PUT_FILES_RETRY_DELAYS)


@responses.activate
def test_put_files_raises_when_the_app_stays_unavailable(sleeps):
    responses.add_callback(responses.PUT, FILES_URL, callback=_replies(503, 503, 503))
    sx = ServiceXAdapter("http://servicex.org/", '12345')

    with pytest.raises(requests.exceptions.HTTPError):
        sx.put_files(_files(1))
    assert len(responses.calls) == 3


@responses.activate
def test_put_files_raises_when_the_app_stays_unreachable(sleeps):
    responses.add_callback(responses.PUT, FILES_URL, callback=_replies(
        *[requests.exceptions.ConnectionError("refused")] * 3))
    sx = ServiceXAdapter("http://servicex.org/", '12345')

    with pytest.raises(requests.exceptions.ConnectionError):
        sx.put_files(_files(1))
    assert len(responses.calls) == 3


@responses.activate
def test_put_files_does_not_retry_a_client_error(sleeps):
    responses.add_callback(responses.PUT, FILES_URL, callback=_replies(400))
    sx = ServiceXAdapter("http://servicex.org/", '12345')

    with pytest.raises(requests.exceptions.HTTPError):
        sx.put_files(_files(1))
    assert len(responses.calls) == 1
    assert sleeps == []


@responses.activate
def test_put_files_resends_only_the_failed_chunk(sleeps):
    responses.add_callback(responses.PUT, FILES_URL, callback=_replies(200, 503, 200))
    sx = ServiceXAdapter("http://servicex.org/", '12345')

    assert sx.put_files(_files(3), chunk_length=2) == 7
    sent = [[f['file_key'] for f in json.loads(c.request.body)] for c in responses.calls]
    assert sent == [['foo/file0', 'foo/file1'], ['foo/file2'], ['foo/file2']]


@responses.activate
def test_put_files_needs_max_file_id(sleeps):
    responses.add(responses.PUT, FILES_URL, json={})
    sx = ServiceXAdapter("http://servicex.org/", '12345')

    with pytest.raises(ValueError, match="max_file_id"):
        sx.put_files(_files(1))


@responses.activate
def test_put_files_returns_max_file_id():
    ids = iter([7, 12])
    responses.add_callback(
        responses.PUT, 'http://servicex.org/12345/files',
        callback=lambda request: (200, {}, json.dumps({"dataset_id": "12345",
                                                       "max_file_id": next(ids)})))
    sx = ServiceXAdapter("http://servicex.org/", '12345')
    files = [{'paths': [f"f{i}"], 'adler32': 'a', 'file_size': 1, 'file_events': 2,
              'file_key': f"key{i}"} for i in range(3)]

    assert sx.put_files(files, chunk_length=2) == 12
    assert len(responses.calls) == 2
    submitted = json.loads(responses.calls[0].request.body)
    assert [f['file_key'] for f in submitted] == ["key0", "key1"]
    assert submitted[0]['paths'] == ["f0"]


@responses.activate
def test_put_files_raises_on_http_error():
    responses.add(responses.PUT, 'http://servicex.org/12345/files', status=500)
    sx = ServiceXAdapter("http://servicex.org/", '12345')
    files = [{'paths': ["f"], 'adler32': 'a', 'file_size': 1, 'file_events': 2,
              'file_key': "f"}]
    with pytest.raises(requests.exceptions.HTTPError):
        sx.put_files(files)
    assert len(responses.calls) == 1  # Temporal retries, not the adaptor


@responses.activate
def test_put_files_requires_max_file_id():
    responses.add(responses.PUT, 'http://servicex.org/12345/files',
                  json={"dataset_id": "12345"})
    sx = ServiceXAdapter("http://servicex.org/", '12345')
    files = [{'paths': ["f"], 'adler32': 'a', 'file_size': 1, 'file_events': 2,
              'file_key': "f"}]
    with pytest.raises(ValueError):
        sx.put_files(files)
