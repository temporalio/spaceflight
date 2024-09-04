import concurrent
import csv
import logging
import asyncio
from datetime import timedelta
import threading

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

from spaceflight.sensor_data import (
    SampleSensorData,
    TelemetryData,
    parse_space_data_line,
    parse_time,
    write_data_periodically,
)


class SpaceActivities:
    def __init__(self, data_file: str) -> None:
        self.data_file = data_file

    @activity.defn
    def obtain_telem_data(self, last_read_str: str) -> TelemetryData:
        last_read = parse_time(last_read_str) if last_read_str else None
        records = []

        with open(self.data_file, "r", newline="") as f:
            reader = csv.reader(f, delimiter=",")
            for row in reader:
                record = parse_space_data_line(row)
                if record is None:
                    print(f"Failed to parse line: {row}")
                    continue
                if last_read is not None:
                    # Only include entries newer than last_read
                    try:
                        if parse_time(record.time) > last_read:
                            records.append(record)
                    except Exception:
                        print(f"Could not parse time: {record.time}")
                else:
                    records.append(record)

        activity.logger.info(f"Found {len(records)} new data entries")
        last_read = records[-1].time if records else None
        return TelemetryData(last_read=last_read, read_records=records)


@workflow.defn
class SpaceWorkflow:
    @workflow.run
    async def run(self) -> str:
        workflow.logger.info("Running workflow")
        last_time = ""
        while True:
            try:
                res = await workflow.execute_local_activity(
                    SpaceActivities.obtain_telem_data,
                    last_time,
                    start_to_close_timeout=timedelta(seconds=10),
                )
                last_time = res.last_read
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
        return "Landed!"


async def main():
    data_file = "/tmp/sensor_data.csv"
    logging.basicConfig(level=logging.INFO)
    space_activities = SpaceActivities(data_file)

    # Start up the thread for writing sensor data to the file in the background
    interrupt_event = threading.Event()
    data_thread = threading.Thread(
        target=write_data_periodically,
        args=(interrupt_event, SampleSensorData(), data_file),
    )
    data_thread.start()

    while True:
        print("Attempting to connect to Temporal")
        try:
            client = await Client.connect("localhost:7233")
            print("Connected to Temporal")
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=4
            ) as activity_executor:
                worker = Worker(
                    client,
                    task_queue="temporal-in-space",
                    workflows=[SpaceWorkflow],
                    activities=[space_activities.obtain_telem_data],
                    activity_executor=activity_executor,
                )
                await worker.run()
        except RuntimeError:
            print("Error connecting or with worker")
            await asyncio.sleep(5)
            continue
        except KeyboardInterrupt:
            print("Shutting down")
            interrupt_event.set()
            data_thread.join()


if __name__ == "__main__":
    asyncio.run(main())
