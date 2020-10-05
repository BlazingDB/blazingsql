import logging


class logging_handler (logging.Handler):
    def __init__(self):
        self.log = []
        super().__init__()

    def emit(self, record):
        self.log.append(record)
