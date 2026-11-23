# ServiceX_DID

 ServiceX DID Library

## Introduction

ServiceX DID finders take a dataset name and turn them into files to be transformed. They are 
implemented as a Celery application with a task called `do_lookup`. Developers of a specific
DID Finder implementation need to write a generator function which yields a dictionary for each
file in the dataset.

The Task interacts with ServiceX through the App's REST endpoint to add files to the dataset and
a separate REST endpoint to signal that the dataset is complete.

The app caches DID lookups. The `dataset_id` is the primary key for the cache table.

Invocations of the `do_lookup` task accepts the following arguments:
* `did`: The dataset identifier to look up
* `dataset_id`: The ID of the dataset in the database
* `endpoint`: The ServiceX endpoint to send the results to
* `user_did_finder`: The user callback that is a generator function that yields file information dictionaries.

## Creating a DID Finder
You start with a new Python project. You will need to add this library as a dependency to the project
by adding the following to your `pyproject.tom` file:

```
servicex-did-finder-lib = "^3.0"
```

Create a celery app that will run your DID finder. This app will be responsible for starting the
Celery worker and registering your DID finder function as a task. Here is an example of how to do
this. Celery prefers that the app is in a file called `celery.py` in a module in your project. Here
is an example of how to do this:

## celery.py:
```python

from servicex_did_finder_lib import DIDFinderApp
rucio_adaptor = RucioAdaptor()
app = DIDFinderApp('rucio', did_finder_args={"rucio_adapter": rucio_adaptor})
```

Attach the DID finder to the app by using the `did_lookup_task` decorator. This decorator will
register the function as a Celery task. Here is an example of how to do this:

```python
@app.did_lookup_task(name="did_finder_rucio.lookup_dataset")
def lookup_dataset(self, did: str, dataset_id: int, endpoint: str) -> None:
    self.do_lookup(did=did, dataset_id=dataset_id,
                   endpoint=endpoint, user_did_finder=find_files)
```

You will need to implement the `find_files` function. This function is a generator that yields
file information dictionaries.

The arguments to the method are straight forward:

* `did_name`: the name of the DID that you should look up. It has the schema stripped off (e.g. if the user sent ServiceX `rucio://dataset_name_in_rucio`, then `did_name` will be `dataset_name_in_rucio`)
* `info` contains a dict of various info about the database ID for this dataset.
* `did_finder_args` contains the arguments that were passed to the DID finder at startup. This is a way to pass command line arguments to your file finder

Yield the results as you find them. The fields you need to pass back to the library are as follows:

* `paths`: An ordered list of URIs that a transformer in ServiceX can access to get at the file. Often these are either `root://` or `http://` schema URI's. When accessing the file, URIs will be tried in ordered listed.
* `adler32`: A CRC number for the file. This CRC is calculated in a special way by rucio and is not used. Leave as 0 if you do not know it.
* `file_size`: Number of bytes of the file. Used to calculate statistics. Leave as zero if you do not know it (or it is expensive to look up).
* `file_events`: Number of events in the file. Used to calculate statistics. Leave as zero if you do not know it (or it is expensive to look up).
* `file_key` (optional, used by the Temporal worker): A stable identity for the file that does not change between lookups, e.g. Rucio `scope:name`. ServiceX dedupes files on it. Defaults to the first entry in `paths`.

Here's a simple example of a did handler generator:

```python
def find_files(did_name: str,
               info: Dict[str, Any],
               did_finder_args: Dict[str, Any]
               ) -> Generator[Dict[str, Any], None]:
    __log.info('DID Lookup request received.', extra={
        'dataset_id': info['dataset-id'], 'dataset': did_name})

    urls = xrd.glob(cache_prefix + did_name)
    if len(urls) == 0:
        raise NoSuchDatasetException(f"No files found matching {did_name} for dataset "
                                     f"{info['dataset-id']} - are you sure it is correct?")

    for url in urls:
        yield {
            'paths': [url],
            'adler32': 0,  # No clue
            'file_size': 0,  # We could look up the size but that would be slow
            'file_events': 0,  # And this we do not know
        }
```


### Errors
Raise one of the exceptions in `servicex_did_finder_lib.exceptions` to tell ServiceX why a
lookup failed. Any other exception is treated as an internal failure.

| Exception | Celery dataset status | Temporal error type | Retried under Temporal |
|---|---|---|---|
| `NoSuchDatasetException` | `does_not_exist` | `DatasetNotFound` | no |
| `BadDatasetNameException` | `bad_name` | `BadDID` | no |
| `AuthFailedException` | `internal_failure` | `AuthFailed` | no |
| `LookupFailureException` or anything else | `internal_failure` | `LookupFailed` | yes |

## Running as a Temporal worker
ServiceX is moving from Celery to Temporal. The same `find_files` generator can run as a
Temporal worker with `DIDFinderWorker`, which hosts the `lookup_dataset` activity that the
ServiceX orchestrator's `DatasetLookupWorkflow` schedules. `DIDFinderApp` (Celery) is kept
for one release while ServiceX cuts over.

```python
from servicex_did_finder_lib import DIDFinderWorker

if __name__ == "__main__":
    DIDFinderWorker("rucio", find_files, did_finder_args={"rucio_adapter": rucio_adaptor}).run()
```

The first argument is the DID scheme the finder serves. `run()` connects to Temporal (waiting
until it is reachable), polls the `did-finder-<scheme>` task queue, and drains in-flight lookups
on SIGTERM. Settings not passed to the constructor come from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal frontend |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `did-finder-<scheme>` | Task queue to poll |
| `SERVICEX_INTERNAL_ENDPOINT` | (required) | The app's internal endpoint, e.g. `http://servicex-app:8000/servicex/internal/transformation/` |
| `DID_FINDER_MAX_CONCURRENT` | `4` | Lookups this worker runs at once |

How a lookup differs from the Celery path:

* Files are sent to ServiceX in batches (300 files, or whatever has accumulated after 5 seconds).
  After each batch the activity heartbeats and signals the lookup workflow, so transforms can
  start on the first files while the lookup continues.
* `find_files` runs in a thread, so blocking catalog calls are fine. The activity keeps
  heartbeating while it waits.
* The orchestrator, not the finder, marks the dataset complete or failed. Errors are raised to
  Temporal, which retries everything except the non-retryable types above.
* A retried lookup starts over. `info['resume']` holds `{"files_written", "last_file_key"}` from
  the last batch the previous attempt committed (or `None`); a finder that lists files in a
  stable order may use it to skip ahead. ServiceX dedupes on `file_key` either way.
* `info` also carries `did-scheme` and `did-hash` alongside `dataset-id`.

Requires Python 3.11 or later.

## Extra Command Line Arguments
Sometimes you need to pass additional information to your DID Finder from the command line. You do
this by creating your own `ArgParser` 
```python
import argparse
# Parse command-line arguments
parser = argparse.ArgumentParser(description='DIDFinderApp')
parser.add_argument('--custom-arg', help='Custom argument for DIDFinderApp')
args, unknown = parser.parse_known_args()

# Create the app instance
app = DIDFinderApp('myApp', did_finder_args={"custom-arg": args.custom_arg})

```

These parsed args will be passed to your `find_files` function as a dictionary in 
the `did_finder_args` parameter.


### Proper Logging

In the end, all DID finders for ServiceX will run under Kubernetes. ServiceX comes with a built in logging mechanism. If anything is to be logged it should use the log system using the python standard `logging` module, with some extra information. For example, here is how to log a message from your callback function:

```python
    import logger
    __log = logger.getLogger(__name__)
    async def my_callback(did_name: str, info: Dict[str, Any]):
        __log.info(f'Looking up dataset {did_name}.',
                     extra={'somethign': info['something']})

        for i in range(0, 10):
            yield {
                'paths': [f"root://atlas-experiment.cern.ch/dataset1/file{i}.root"]
                'adler32': b183712731,
                'file_size': 0,
                'file_events': 0,
            }
```

The `DIDFinderApp` will configure the python root logger properly.

## URI Format

All the incoming DID's are expected to be URI's without the schema. As such, there are two parameters that are currently parsed by the library. The rest are let through and routed to the callback:

* `files` - Number of files to report back to ServiceX. All files from the dataset are found, and then sorted in order. The first n files are then
    sent back. Default is all files.
* `get` - If the value is `all` (the default) then all files in the dataset must be returned. If the value is `available`, then only files that are accessible need be returned.

As am example, if the following URI is given to ServiceX, "rucio://dataset_name?files=20&get=available", then the first 20 available files of the dataset will be processed by the rest of servicex.

## Stressful DID Finder
As an example, there is in this repo a simple DID finder that can be used to test the system. It is called `stressful_did_finder.py`. It will return a large number of files, and will take a long time to run. It is useful for testing the system under load.
I'm not quite sure how to use it yet, but I'm sure it will be useful.

It accepts the following arguments:
* `--num-files` - The number of files to return as part of each request. Default is 10.
* `--file-path` - The DID Finder returns the same file over and over. This is the file to return in the response
