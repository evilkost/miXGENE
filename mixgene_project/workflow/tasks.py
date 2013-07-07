import cPickle as pickle

from celery import task
from mixgene.util import get_redis_instance

"""
Reserved fields in context:
'exp_id' : experiment id,

"""
class AtomicTask(object):
    def __init__(self, name, func, inmap, outmap):
        self.kind = 'atomic'
        self.name = name
        self.func = func

        self.inmap = inmap   #    outer_name -> local_name
        self.outmap = outmap #    local_name -> outer_name

    def do(self, ctx):
        params = {}
        for name, local_name in self.inmap.iteritems():
            params[local_name] = ctx[name]

        raw_res = self.func(params)
        res = {}
        for local_name, name in self.outmap.iteritems():
            res[name] = raw_res[local_name]
        ctx.update(res)
        return ctx

class SeqTask(object):
    def __init__(self, name, subtasks):
        self.kind = 'seq'
        self.name = name
        self.subtasks = subtasks

class ParTask(object):
    def __init__(self, name, subtasks):
        self.kind = 'par'
        self.name = name
        self.subtasks = subtasks

@task(name='workflow.tasks.exc_task')
def exc_task(ctx, task_obj, c_subtask):
    if task_obj.kind == 'atomic':
        res = task_obj.do(ctx)
        ctx.update(res)
        c_subtask.apply_async((ctx,))
    elif task_obj.kind == 'seq':
        print "calling exc seq"
        exc_seq.s(ctx, task_obj, 0, c_subtask).apply_async()
    elif task_obj.kind == 'par':
        print "calling exc seq"
        exc_par.s(ctx, task_obj, c_subtask).apply_async()
    else:
        print "Shouldn't be there"


@task(name='worflow.tasks.exc_seq')
def exc_seq(ctx, seq_task, num, c_subtask):
    # TODO: update context after each subtask
    print "enter exc_seq, name %s, num %s" % (seq_task.name, num)
    if len(seq_task.subtasks) == num:
        c_subtask.apply_async((ctx, ))
    else:
        next_in_chain = exc_seq.s(seq_task, num + 1, c_subtask)
        exc_task.s(ctx, seq_task.subtasks[num], next_in_chain).apply_async()


PAR_REDIS_PREFIX_RETURN_SUBTASK="PRPR:"
PAR_REDIS_PREFIX_DONE="PRPD:"
PAR_REDIS_PREFIX_RESULT_CONTEXT="PRPC:"
@task(name='workflow.tasks.par_collect')
def par_collect(ctx, pre_ctx, subtask_name, parent_task):
    r = get_redis_instance()
    key_done = PAR_REDIS_PREFIX_DONE + ctx['exp_id']
    key_context = PAR_REDIS_PREFIX_RESULT_CONTEXT + ctx['exp_id'] + ":" + subtask_name
    member = subtask_name
    r.zadd(key_done, member, 1)
    r.set(key_context, pickle.dumps(ctx) )

    entire_zset = r.zrange(key_done, 0, -1, withscores=True)
    print entire_zset
    if len(filter(lambda x: x == 1, [v for k,v in entire_zset])) == len(parent_task.subtasks):
        ctx = {}
        ctx.update(pre_ctx)
        for st in parent_task.subtasks:
            key_context = PAR_REDIS_PREFIX_RESULT_CONTEXT + ctx['exp_id'] + ":" + st.name
            res_ctx = pickle.loads(r.get(key_context))
            ctx.update(res_ctx)

        key_subtask = PAR_REDIS_PREFIX_RETURN_SUBTASK + ctx['exp_id'] + ":" + parent_task.name
        c_subtask = pickle.loads(r.get(key_subtask))
        c_subtask.apply_async((ctx, ))


@task(name='workflow.tasks.exc_par')
def exc_par(ctx, par_task, c_subtask):
    print "enter exc_par, name %s" % (par_task.name,)
    r = get_redis_instance()
    key_subtask = PAR_REDIS_PREFIX_RETURN_SUBTASK + ctx['exp_id'] + ":" + par_task.name
    csbp = pickle.dumps(c_subtask)
    #print "%s, %s" % (key_subtask, csbp)
    r.set(key_subtask, csbp )
    for st in par_task.subtasks:
        cb_subtask = par_collect.s(ctx, st.name, par_task)
        exc_task.s(ctx, st, cb_subtask).apply_async()


### Usage example
import time

@task(name='workflow.tasks.xprint')
def xprint(context):
    print "xprint context: %s" % context

def xadd(context):
    ctx = {}
    #ctx.update(context)  # Not sure if this is correct action
    #ctx['a'] = context['a'] + context['b']
    ctx[2] = context[1] + context[2]
    print 'xadd context', ctx
    time.sleep(2)
    print 'xadd context finished'
    return ctx

def usage_example():
    at1 = AtomicTask('at:1', xadd, {1: 1, 2: 2}, {2: 'r1'})
    at2 = AtomicTask('at:2', xadd, {1: 1, 2: 2}, {2: 'r2'})
    at3 = AtomicTask('at:3', xadd, {1: 1, 2: 2}, {2: 2})


    ct = SeqTask('ct:1', [at1, at2, at3])
    ct2 = SeqTask('ct:2', [at1, ct, at2])

    pt = ParTask('pt:1', [at1, at2, ct2])

    exc_task.s({1:1, 2:2, 'exp_id': str(time.time())}, pt, xprint).apply_async()


