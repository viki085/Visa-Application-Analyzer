import sys
from visa.logger import logging
from visa.exception import VisaException

# logging.info("This is a test log message from demo.py")

try:
    a = 1 / 0
except Exception as e:
    logging.info("An error occurred in demo.py")
    raise VisaException(e, sys)
