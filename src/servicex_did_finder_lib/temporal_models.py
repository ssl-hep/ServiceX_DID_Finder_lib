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
"""Wire format shared with the ServiceX orchestrator's DatasetLookupWorkflow.

The source of truth is servicex_orchestrator/src/servicex_orchestrator/models.py in the
ServiceX repo. Temporal serializes these dataclasses as JSON by field name, so the names
here must match it exactly.
"""
from dataclasses import dataclass

# Name of the activity the orchestrator schedules, and the signal it listens for
LOOKUP_ACTIVITY_NAME = "lookup_dataset"
PROGRESS_SIGNAL = "progress"

# ApplicationError types. The orchestrator does not retry the first three.
DATASET_NOT_FOUND = "DatasetNotFound"
BAD_DID = "BadDID"
AUTH_FAILED = "AuthFailed"
LOOKUP_FAILED = "LookupFailed"

NON_RETRYABLE_ERROR_TYPES = frozenset({DATASET_NOT_FOUND, BAD_DID, AUTH_FAILED})


@dataclass
class LookupInput:
    did_hash: str
    did: str
    did_scheme: str
    dataset_id: int


@dataclass
class LookupSummary:
    n_files: int
    max_file_id: int
    size_bytes: int = 0
    events: int = 0
