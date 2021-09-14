import re


def validate_date(date: str) -> None:
    """
    Checks date for format YYY-MM-DD. Raises ValueError when date in wrong format
    @param date: date as string
    @return:
    """
    if not isinstance(date, str):
        raise ValueError(f"{date=} must be of type str, not {type(str)}")

    rex = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    if not rex.match(date) or not 0 < int(date.split("-")[1]) <= 12 or not 0 <= int(date.split("-")[2]) <= 31:
        raise ValueError(f"{date=} must be of format YYYY-MM-DD")


def validate_date_range(date_range: list) -> None:
    if not isinstance(date_range, list):
        raise ValueError(f"{date_range=} must be of type list, not {type(date_range)}")

    if len(date_range) != 2:
        raise ValueError(f"{date_range=} must have length 2, not {len(date_range)}")

    for date in date_range:
        validate_date(date)


def validate_resolution(resolution: str) -> None:
    known_resolution = ["COMPLETE", "YEARS", "QUARTERS", "DAYS"]
    if resolution not in known_resolution:
        raise ValueError(f"Unknown {resolution=}. Must be in {known_resolution}")


def validate_time_unit(time_unit: str):
    valid_time_units = ['QUARTERS', 'DAYS']
    if time_unit not in valid_time_units:
        raise ValueError(f"{time_unit=} not in {valid_time_units}")


def validate_time_count(time_count: int):
    if time_count < 0:
        raise ValueError(f"{time_count=} must be positiv.")


def validate_index_selector(index_selector: str):
    valid_index_selectors = ['EARLIEST', 'LATEST', 'RANDOM']
    if index_selector not in valid_index_selectors:
        raise ValueError(f"{index_selector=} not in {valid_index_selectors}.")


def validate_index_plament(index_placement: str):
    valid_index_placements = ['BEFORE', 'NEUTRAL', 'AFTER']

    if index_placement not in valid_index_placements:
        raise ValueError(f"{index_placement=} not in {valid_index_placements}.")


