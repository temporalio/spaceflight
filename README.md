# Temporal Worker in Spaaaaaaaaaaaaaace

A Python Temporal worker, in space!

This is a simple Python worker that uses the Temporal SDK to run a worker...
in space! A background thread periodically writes sensor data to a file, which
is then read from a local activity inside the workflow, attempting to publish
any unpublished data since the last time it ran.

When the worker loses connectivity, the workflow won't be able to make
progress until it reconnects, which will happen automatically. Once it does,
any unwritten data will be flushed to the workflow as the result of the local activity.

Note that this application is not meant to be demonstrate Temporal best practices.
It would be much simpler for the Pi to simply periodically send signals rather than
running a whole worker. However, running the worker makes for a fun demonstration of
Temporal's ability to survive network partitions.

## Running

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install temporalio
pip install pyserial
```

Run the worker, providing the serial device as an argument. Do not provide the device path to
test with sample data:
```bash
python src/spaceflight/space_worker.py /dev/ttyUSB0
```

If this prints something like "permission denied", it might need to be run with `sudo` to have
permissions for the serial port.

Use ctrl-c to stop the script
