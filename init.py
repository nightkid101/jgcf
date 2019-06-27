import logging
import configparser
import os


# get all config
def config_init():
    config = configparser.ConfigParser()
    config.read('/home/wangl/jgcf/config.ini')
    return config


# get logger
def logger_init(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # output to console
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # output to file
    if not os.path.exists('./logs'):
        os.mkdir('./logs')
    log_file_name = os.path.join('logs', '%s.log' % logger_name)
    file_handler = logging.FileHandler(filename=log_file_name, encoding='utf-8', mode='w')
    file_formatter = logging.Formatter('%(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger
