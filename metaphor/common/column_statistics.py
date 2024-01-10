from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class ColumnStatistics:
    # Compute null and non-null counts
    null_count: bool = True

    # Compute unique value counts
    unique_count: bool = False

    # Compute min value
    min_value: bool = True

    # Compute max value
    max_value: bool = True

    # Compute average value
    avg_value: bool = False

    # Compute standard deviation
    std_dev: bool = False

    @property
    def should_calculate(self) -> bool:
        return any(bool(value) for value in self.__dict__.values())
