import logging

logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)

# create file handler
file_handler = logging.FileHandler("warpy.log")
file_handler.setLevel(logging.INFO)

# create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# create formatter and add it to the handlers
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
