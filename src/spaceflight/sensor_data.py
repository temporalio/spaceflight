from dataclasses import dataclass
import datetime
import threading
from abc import ABC, abstractmethod
from typing import List
import serial


@dataclass
class TelemetryRecord:
    time: str  # time (no date) as isoformat
    data_point: int
    gps_longitude: float
    gps_latitude: float
    altitude: int


@dataclass
class TelemetryData:
    last_read: str | None  # datetime as isoformat
    read_records: list[TelemetryRecord]


class SensorData(ABC):
    @abstractmethod
    def get_available_data(self) -> str:
        pass


class USBSensorData(SensorData):
    def __init__(self, device: str):
        self.device = device

    def get_available_data(self) -> str:
        # TODO: No idea what the baudrate is
        with serial.Serial(self.device, 9600, timeout=2) as ser:
            data = ser.readlines()
        return "".join([line.decode("utf-8", errors="replace") for line in data])


class SampleSensorData(SensorData):
    def __init__(self):
        with open("sample-data.csv") as f:
            self.data = f.readlines()
        self.index = 0

    def get_available_data(self) -> str:
        line = self.data[self.index : self.index + 10]
        self.index += 10
        return "".join(line)


def write_data_periodically(
    interruptor: threading.Event, provider: SensorData, file: str
):
    with open(file, "a", newline="") as f:
        while True:
            line = provider.get_available_data()
            f.write(line)
            f.flush()
            if interruptor.wait(10):
                break


def parse_space_data_line(line: List[str]) -> TelemetryRecord | None:
    """
    Deal with a CSV-parsed line of space data and turn it into a TelemetryRecord object.

    Data looks like:
        $$SIS-525,8,21-5-24,9:40:28,[NO GPS LOCK AVAILABLE]
        $$SIS-525,17,21-5-24,9:41:12,53.389080,-1.437518,5,76,0*4394
    """
    if len(line) < 4:
        return None
    try:
        gps_latitude = float("nan")
        gps_longitude = float("nan")
        altitude = 0
        if len(line) > 5:
            gps_latitude = float(line[4])
            gps_longitude = float(line[5])
            altitude = int(line[7])
        return TelemetryRecord(
            data_point=int(line[1]),
            # The D/M/Y format used in the data is in a nonsense format.
            # This is all going to happen today, so, only bothering with timestamp.
            # Time is also in a nonstandard format without leading zeros.
            time=line[3],
            gps_longitude=gps_longitude,
            gps_latitude=gps_latitude,
            altitude=altitude,
        )
    except Exception:
        return None


def parse_time(ts: str) -> datetime.time:
    return datetime.datetime.strptime(ts, "%H:%M:%S").time()
