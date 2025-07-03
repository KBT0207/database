
import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime
import pytz

class TZFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, tz=None):
        super().__init__(fmt, datefmt)
        self.tz = tz or pytz.utc

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

today_date = datetime.now().strftime('%d-%b-%Y').replace(':', '-')

log_directory = os.path.join(os.getcwd(), 'logs')
log_file = os.path.join(log_directory, f"main_{today_date}.log")

if not os.path.exists(log_directory):
    os.makedirs(log_directory)

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            '()': TZFormatter,
            'fmt': '%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S,%f',
            'tz': pytz.timezone("Asia/Kolkata"),
        }
    },
    'handlers': {
        'console_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO'
        },
        'file_handler': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': log_file,
            'when': 'midnight',
            'interval': 1,
            'backupCount': 30,
            'formatter': 'standard',
            'encoding': 'utf-8',
        }
    },
    'loggers': {
        '': {
            'handlers': ['console_handler', 'file_handler'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger("main")


