import datetime
import random
import threading
import csv
from abc import ABC, abstractmethod


class SensorData(ABC):
    @abstractmethod
    def get_temperature(self) -> int:
        pass

    @abstractmethod
    def get_altitude(self) -> int:
        pass


class FakeSensorData(SensorData):
    def get_temperature(self) -> int:
        return random.randint(0, 100)

    def get_altitude(self) -> int:
        return random.randint(0, 10_000)


def write_data_periodically(
    interruptor: threading.Event, provider: SensorData, file: str
):
    with open(file, "a", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        while True:
            writer.writerow(
                [
                    datetime.datetime.now(),
                    provider.get_temperature(),
                    provider.get_altitude(),
                ]
            )
            f.flush()
            if interruptor.wait(10):
                break
