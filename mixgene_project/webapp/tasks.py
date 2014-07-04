# -*- coding: utf-8 -*-
import logging
import redis_lock
from time import time
from celery.task import task
from mixgene.redis_helper import ExpKeys
from mixgene.util import get_redis_instance
from webapp.scope import LOCK_TIME, ScopeRunner
from webapp.notification import AllUpdated, NotifyMode

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

@task(name="webapp.tasks.auto_exec")
def auto_exec_task(exp, scope_name, is_init=False):
    r = get_redis_instance()

    lock_key = ExpKeys.get_auto_exec_task_lock_key(exp.pk, scope_name)
    with redis_lock.Lock(r, lock_key):
        try:
            sr = ScopeRunner(exp, scope_name)
            sr.execute(is_init)
        except Exception, e:
            log.exception(e)

@task(name="webapp.tasks.halt_execution")
def halt_execution_task(exp, scope_name):
    log.debug("halt execution invoked")

    r = get_redis_instance()

    lock_key = ExpKeys.get_auto_exec_task_lock_key(exp.pk, scope_name)
    with redis_lock.Lock(r, lock_key):
        try:
            if scope_name == "root":
                AllUpdated(
                    exp.pk,
                    comment=u"An error occurred during experiment execution",
                    silent=False,
                    mode=NotifyMode.ERROR
                ).send()
            else:
                block = exp.get_meta_block_by_sub_scope(scope_name)
                block.do_action("error", exp)
        except Exception, e:
            log.exception(e)

@task(name="webapp.tasks.deferred_block_method")
def deferred_block_method(exp, block, method_name, *args, **kwargs):
    getattr(block, method_name)(exp, *args, **kwargs)

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