import time
import datetime

class TimeConverter:
    FACTORS = {
        "minutes": 60 * 1000,
        "hours": 60 * 60 * 1000,
        "days": 24 * 60 * 60 * 1000,
        "weeks": 7 * 24 * 60 * 60 * 1000,
        "months": 30 * 24 * 60 * 60 * 1000,
        "years": 365 * 24 * 60 * 60 * 1000,
    }

    @staticmethod
    def ms_now() -> int:
        return int(round(time.time() * 1000))

    @staticmethod
    def to_ms(factor: str, units: int) -> int:
        return units * TimeConverter.FACTORS[factor]

    @staticmethod
    def from_ms(factor: str, ms: int) -> float:
        return ms / TimeConverter.FACTORS[factor]

    @staticmethod
    def ago_to_unixms(factor: str, units: int) -> int:
        return TimeConverter.ms_now() - TimeConverter.to_ms(factor, units)

    @staticmethod
    def unixms_to_ago(factor: str, ms: int) -> float:
        return TimeConverter.from_ms(factor, TimeConverter.ms_now() - ms)

    @staticmethod
    def ymd_to_unixms(year: int, month: int, day: int) -> int:
        return int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)) * 1000)

    @staticmethod
    def datetime_to_unixms(dt: datetime.datetime) -> int:
        return int(time.mktime(dt.timetuple()) * 1000)