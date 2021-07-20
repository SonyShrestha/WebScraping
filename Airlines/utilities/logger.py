"""
    Author          :   Siddhi
    Created_date    :   2019/08/20  
    Modified Date   :   2019/09/26
    Description     :   Definition for logger.
"""
import os
from utilities.global_vars import *


def get_logger(level, print_to_console=1):
    import logging
    try:
        formatter = "%(asctime)s -- %(pathname)s --%(filename)s-- %(module)s --\
                %(funcName)s -- %(lineno)d-- %(name)s -- %(levelname)s -- %(message)s"
        if not os.path.exists(os.path.abspath(file_path + '/log')):
            os.mkdir(os.path.abspath(file_path+'/log'))
        logging.basicConfig(filename=os.path.abspath(
            file_path+'/log/log.log'), level=level, format=formatter)
        if print_to_console == 1:
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            formatter = logging.Formatter(
                formatter)
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)
            return logging
        elif print_to_console == 0:
            return logging
        else:
            raise ValueError(f"Expected 1 or 0. Got {print_to_console}")
    except Exception as e:
        raise e


logging = get_logger(logging_level, log_to_console)
