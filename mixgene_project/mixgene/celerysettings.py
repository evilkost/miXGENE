# -*- coding: utf-8 -*-
# from pprint import pprint

from settings import *

#LOGGING = {}

# from logging import Formatter


# def celery_formatter_factory(default_fmt, extended_fmt, fields_for_extended, datefmt):
#     class MyFormatter(Formatter):
#         def __init__(self, *args, **kwargs):
#             super(MyFormatter, self).__init__(default_fmt, datefmt, *args, **kwargs)
#             self._extended_formatter = Formatter(extended_fmt, datefmt)
#
#         def format(self, record):
#             if all(hasattr(record, field) for field in fields_for_extended):
#                 s = self._extended_formatter.format(record)
#             else:
#                 s = super(MyFormatter, self).format(record)
#
#             return s + "%s" % record.__dict__.keys()
#
#     return MyFormatter()

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '[%(asctime)s %(processName)s|%(levelname)s][%(module)s:%(lineno)s:%(funcName)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'celery_fmt': {
            'format': '[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        #TODO: add custom formatter which would try to apply celery_fmt if possible
        # 'celery_fmt': {
        #     '()': 'mixgene.celerysettings.celery_formatter_factory',
        #     'default_fmt': '[%(asctime)s|%(levelname)s][%(module)s:%(lineno)s:%(funcName)s] %(message)s',
        #     'extended_fmt': '[%(asctime)s: %(levelname)s][%(task_name)s(%(task_id)s)] %(message)s',
        #     'fields_for_extended': ['task_name', 'task_id'],
        #     'datefmt': '%Y-%m-%d %H:%M:%S',
        # },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'handlers': {
        'celery': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'verbose',
            'filename': LOG_DIR + '/celery.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 3,
        },
        'celery_task': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'verbose',
            'filename': LOG_DIR + '/celery_task.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 3,
        },
        'console': {
            'level': 'DEBUG',
            'formatter': 'verbose',
            'class': 'logging.StreamHandler'
        },
    },
    'loggers': {
        'celery': {
            'handlers': ['celery', ],
            'level': 'INFO',
            'propagate': False,
        },
        '': {
            'handlers': ['celery_task', 'console'],
            'level': 'DEBUG',
            'propagate': True,
        },
    }
}
