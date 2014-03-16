# -*- coding: utf-8 -*-
import logging
from time import time
from celery.task import task
from mixgene.redis_helper import ExpKeys
from mixgene.util import get_redis_instance
from webapp.scope import LOCK_TIME, ScopeRunner

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

@task(name="webapp.tasks.auto_exec")
def auto_exec_task(exp, scope_name, is_init=False):
    r = get_redis_instance()

    lock_key = ExpKeys.get_auto_exec_task_lock_key(exp.pk, scope_name)
    lock = r.setnx(lock_key, str(int(time()) + LOCK_TIME))
    if lock:
        try:
            sr = ScopeRunner(exp, scope_name)
            sr.execute(is_init)
        except Exception, e:
            log.exception(e)
        finally:
            r.delete(lock_key)

@task(name="webapp.tasks.wrapper_task")
def wrapper_task(func, exp, block,
             # success_action="success", error_action="error",
             *args, **kwargs
    ):
    success_action = kwargs.pop("success_action", "success")
    error_action = kwargs.pop("error_action", "error")
    # log = wrapper_task.get_logger()

    try:
        log.info("trying to apply: %s", func)
        result_list, key_result = func(exp, block, *args, **kwargs)
        block.do_action(success_action, exp, *result_list, **key_result)
    except Exception, e:
        log.exception(e)
        block.do_action(error_action, exp, e)