import concurrent
import sys
import csv
import logging
import asyncio
import queue
from datetime import timedelta
import threading

from temporalio import activity, workflow
from temporalio.worker import Worker

from client import get_client
from sensor_data import (
    SampleSensorData,
    TelemetryData,
    USBSensorData,
    parse_space_data_line,
    parse_time,
    write_data_periodically,
)


class SpaceActivities:
    def __init__(self, data_queue: queue.Queue) -> None:
        self.data_queue = data_queue

    @activity.defn
    def obtain_telem_data(self, last_read_str: str) -> TelemetryData:
        last_read = parse_time(last_read_str) if last_read_str else None
        records = []

        all_lines = []
        while not self.data_queue.empty():
            all_lines.append(self.data_queue.get(block=False))

        reader = csv.reader(all_lines, delimiter=",")
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
                if res.last_read:
                    last_time = res.last_read
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
        return "Landed!"


async def main():
    data_queue = queue.Queue()
    logging.basicConfig(level=logging.INFO)
    space_activities = SpaceActivities(data_queue)

    datasource = sys.argv[1] if len(sys.argv) > 1 else "sample"
    print(f"Using data source: {datasource}")
    if datasource == "sample":
        datasource = SampleSensorData()
    else:
        datasource = USBSensorData(datasource)
    # Start up the thread for writing sensor data to the queue in the background
    interrupt_event = threading.Event()
    data_thread = threading.Thread(
        target=write_data_periodically,
        args=(interrupt_event, datasource, data_queue),
    )
    data_thread.start()

    while True:
        try:
            client = await get_client()
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
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("Shutting down")
            interrupt_event.set()
            data_thread.join()
            break
        except (Exception, RuntimeError) as e:
            print(f"Error connecting or running worker: {e}")
            await asyncio.sleep(5)
            continue


if __name__ == "__main__":
    asyncio.run(main())
