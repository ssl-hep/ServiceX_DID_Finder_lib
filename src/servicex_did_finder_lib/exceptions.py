# Copyright (c) 2025, IRIS-HEP
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

# Define some exceptions to indicate various problems in DID finding
from temporalio.exceptions import ApplicationError

from servicex_did_finder_lib.temporal_models import (
    AUTH_FAILED,
    BAD_DID,
    DATASET_NOT_FOUND,
    LOOKUP_FAILED,
    NON_RETRYABLE_ERROR_TYPES,
)


class BaseDIDFinderException(Exception):
    """ Base exception type

    error_type is the dataset status reported to ServiceX by the Celery
    DIDFinderApp. temporal_type is the ApplicationError type raised by the
    Temporal DIDFinderWorker; the orchestrator does not retry the types in
    NON_RETRYABLE_ERROR_TYPES.
    """
    transient: bool = True
    error_type: str = ""
    temporal_type: str = LOOKUP_FAILED


class NoSuchDatasetException(BaseDIDFinderException):
    """ The dataset does not exist in the catalog """
    transient = True
    error_type = "does_not_exist"
    temporal_type = DATASET_NOT_FOUND


class BadDatasetNameException(BaseDIDFinderException):
    """ The specified dataset name is invalid """
    transient = False
    error_type = "bad_name"
    temporal_type = BAD_DID


class LookupFailureException(BaseDIDFinderException):
    """ There was a failure when looking up the dataset in the catalog """
    transient = True
    error_type = "internal_failure"
    temporal_type = LOOKUP_FAILED


class AuthFailedException(BaseDIDFinderException):
    """ The DID finder could not authenticate to the catalog (e.g. a bad x509 proxy).
    Not retried under Temporal, so a bad credential does not hammer the catalog. """
    transient = False
    error_type = "internal_failure"
    temporal_type = AUTH_FAILED


def to_application_error(e: BaseException) -> ApplicationError:
    """Convert an exception raised by a DID finder into the ApplicationError the
    orchestrator's DatasetLookupWorkflow classifies by type."""
    if isinstance(e, ApplicationError):
        return e
    error_type = e.temporal_type if isinstance(e, BaseDIDFinderException) else LOOKUP_FAILED
    return ApplicationError(
        f"{type(e).__name__}: {e}",
        type=error_type,
        non_retryable=error_type in NON_RETRYABLE_ERROR_TYPES,
    )
