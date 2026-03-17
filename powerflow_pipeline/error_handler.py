import functools
import traceback


def input_transform_error_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "row" in kwargs:
                row = kwargs["row"]
                formatted_row = row.to_frame().style.format({0: "{:,.0f}"}).to_string()
            else:
                formatted_row = "Input row not available"
            if "device_type" in kwargs:
                device_type = kwargs["device_type"]
            else:
                device_type = "device_type not known"
            with open("error_log.txt", "a") as f:
                f.write(f"--- Exception Occurred while creating `{device_type}`---\n")
                f.write(f"--- INPUT ROW---\n")
                f.write(formatted_row)
                f.write(f"Exception Type: {type(e).__name__}\n")
                f.write(f"Exception Message: {e}\n")
                traceback.print_exc(file=f)
                f.write("\n")

            return None

    return wrapper

def log_method_calls(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        method_name = func.__name__
        arg_strings = []
        if args and args[0].__class__.__name__ != 'function': # Exclude 'self' for instance methods
            args_to_print = args[1:] if hasattr(args[0], method_name) and callable(getattr(args[0], method_name)) else args
            arg_strings.extend(repr(arg) for arg in args_to_print)
        else:
            arg_strings.extend(repr(arg) for arg in args)

        arg_strings.extend(f"{key}={repr(value)}" for key, value in kwargs.items())
        print(f"{method_name}({', '.join(arg_strings)})")
        with open("method_calls.txt", "a") as f:
            f.write(f"{method_name}({', '.join(arg_strings)})\n")
        return func(*args, **kwargs)
    return wrapper
