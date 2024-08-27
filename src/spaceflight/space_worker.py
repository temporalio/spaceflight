import concurrent
import csv
from dataclasses import dataclass
import datetime
import logging
import asyncio
from datetime import timedelta
import threading

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

from spaceflight.sensor_data import FakeSensorData, write_data_periodically


@dataclass
class TelemetryRecord:
    time: str  # datetime as isoformat
    temperature: int
    altitude: int


@dataclass
class TelemetryData:
    last_read: str | None  # datetime as isoformat
    read_records: list[TelemetryRecord]


class SpaceActivities:
    def __init__(self, data_file: str) -> None:
        self.data_file = data_file

    @activity.defn
    def obtain_telem_data(self, last_read_str: str) -> TelemetryData:
        last_read = (
            datetime.datetime.fromisoformat(last_read_str) if last_read_str else None
        )
        records = []

        with open(self.data_file, "r", newline="") as f:
            reader = csv.reader(f, delimiter=",")
            for row in reader:
                timestamp = datetime.datetime.fromisoformat(row[0])
                record = TelemetryRecord(row[0], int(row[1]), int(row[2]))
                if last_read is not None:
                    # Only include entries newer than last_read
                    if timestamp > last_read:
                        records.append(record)
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

    # Start up the thread for writing sensor data to the file in the background
    interrupt_event = threading.Event()
    data_thread = threading.Thread(
        target=write_data_periodically,
        args=(interrupt_event, FakeSensorData(), data_file),
    )
    data_thread.start()

    client = await Client.connect("localhost:7233")
    space_activities = SpaceActivities(data_file)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as activity_executor:
        worker = Worker(
            client,
            task_queue="temporal-in-space",
            workflows=[SpaceWorkflow],
            activities=[space_activities.obtain_telem_data],
            activity_executor=activity_executor,
        )
        try:
            await worker.run()
        finally:
            interrupt_event.set()
            data_thread.join()


if __name__ == "__main__":
    asyncio.run(main())
