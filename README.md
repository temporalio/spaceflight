# Temporal Worker in Spaaaaaaaaaaaaaace

A Python Temporal worker, in space!

This is a simple Python worker that uses the Temporal SDK to run a worker... in space! A background
thread periodically writes sensor data to a file, which is then read from a local activity inside
the workflow, attempting to publish any unpublished data since the last time it ran.

When the worker loses connectivity, the workflow won't be able to make progress until it reconnects,
which will happen automatically. Once it does, any unwritten data will be flushed to the workflow
as the result of the local activity.

## Running

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install temporalio
```

Run the worker:

```bash
python src/spaceflight/space_worker.py
```
