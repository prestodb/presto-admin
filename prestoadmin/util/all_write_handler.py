from logging import handlers
import os


class AllWriteTimedRotatingFileHandler(handlers.TimedRotatingFileHandler):
    def _open(self):
        prev_umask = os.umask(000)
        rotating_file_handler = handlers.TimedRotatingFileHandler._open(self)
        os.umask(prev_umask)
        return rotating_file_handler
