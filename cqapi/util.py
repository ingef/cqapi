from datetime import date
import functools
import inspect


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


def check_arg_type(arguments: list = None, convert_to_list: dict = None):
    def middle(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            args_dict = {arg_name: arg_value
                         for arg_name, arg_value in zip(func.__code__.co_varnames[:len(args)], args)}
            args_dict = {**args_dict, **kwargs}
            for arg_name, arg_parameter in inspect.signature(func).parameters.items():

                if arg_name not in args_dict.keys():
                    continue

                arg_value = args_dict[arg_name]

                # check if value is default value
                default_value = arg_parameter.default
                if not default_value == inspect.Parameter.empty and arg_value == default_value:
                    continue

                # check for list input
                if convert_to_list is not None and arg_name in convert_to_list.keys():
                    if not isinstance(arg_value, list):
                        if not isinstance(arg_value, convert_to_list[arg_name]):
                            raise TypeError(f"Wrong parameter type for function {func.__name__}: \n"
                                            f"{arg_name} is of type {type(arg_value)}, "
                                            f"but must be of type {convert_to_list[arg_name]}. \n"
                                            f"Even better, put it in a list, since the function expects one!")
                        args_dict[arg_name] = [arg_value]

                    elif (bad_types := [type(list_entry) for list_entry in arg_value
                                        if not isinstance(list_entry, convert_to_list[arg_name])]):
                        raise TypeError(f"Wrong parameter type for function {func.__name__}: \n"
                                        f"Entry of list {arg_name} are of type {bad_types}, "
                                        f"but must be of type {convert_to_list[arg_name]}. \n")

                # check arguments
                if arguments is None or arg_name not in arguments:
                    continue
                # type varies from string to typing type, depending on import future.annotations
                if isinstance(arg_parameter.annotation, str):
                    arg_type_val = eval(arg_parameter.annotation)
                else:
                    arg_type_val = arg_parameter.annotation
                if not isinstance(arg_value, arg_type_val):
                    raise TypeError(f"Wrong parameter type for function {func.__name__}: \n"
                                    f"{arg_name} is of type {type(arg_value)}, "
                                    f"but must be of type {arg_type_val}")

            return func(**args_dict)

        return wrapper

    return middle
