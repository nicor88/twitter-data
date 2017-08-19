import logging
from logging.handlers import RotatingFileHandler


def configure_logger(*, name='sender'):
    logging.basicConfig(level=logging.INFO)
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # Logger to rotate the log file every 10MBs and keep 1 backup of previous log file.
    handler = RotatingFileHandler(name + '.log', mode='a', maxBytes=10 * 1000 * 1024,
                                  backupCount=1)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger
