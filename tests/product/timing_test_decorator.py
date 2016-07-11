import logging
import sys

from time import time


logger = logging.getLogger()
logger.setLevel(logging.INFO)
message_format = '%(levelname)s - %(message)s'
formatter = logging.Formatter(message_format)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def log_function_time():
    """
    Returns: Prints the execution time of the decorated function to the
    console. If the execution time exceeds 10 minutes, it will use 'error'
    for the message level. Otherwise, it will use 'info'.
    """
    def name_wrapper(function):
        def time_wrapper(*args, **kwargs):
            global logger
            function_name = function.__name__

            start_time = time()
            return_value = function(*args, **kwargs)
            elapsed_time = time() - start_time

            travis_output_time_limit = 600
            message_level = logging.ERROR if elapsed_time >= travis_output_time_limit \
                else logging.INFO
            logging.disable(logging.NOTSET)
            logger.log(message_level,
                       "%s completed in %s seconds...",
                       function_name,
                       str(elapsed_time))
            logging.disable(logging.CRITICAL)

            return return_value
        return time_wrapper
    return name_wrapper
