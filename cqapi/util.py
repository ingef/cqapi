from datetime import date


def check_input_list(input_list, entry_type=None):
    """
    When type of input_list is not list, it returns list with element input_list.
    When entry_type is set, it throws TypeError, when entry in input_list is not of type entry_type
    """
    if type(input_list) is not list:
        input_list = [input_list]
    if entry_type:
        for entry in input_list:
            if type(entry) is not entry_type:
                raise TypeError(f"Entry {entry} is not of type {entry_type}")
    return input_list


def _parse_iso_date(datestring: str):
    y, m, d = map(lambda x: int(x), datestring.split('-'))
    return date(y, m, d)
