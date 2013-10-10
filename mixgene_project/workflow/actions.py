import cPickle as pickle

from celery import task
from mixgene.util import get_redis_instance
from mixgene.redis_helper import ExpKeys

from webapp.models import Experiment

"""
Reserved fields in context with `exp_` prefix:
'exp_id' : experiment id,
'exp_status' : status of experiment
"""
class AtomicAction(object):
    def __init__(self, name, func, inmap, outmap):
        self.kind = 'atomic'
        self.name = name
        self.func = func

        self.inmap = inmap   #    outer_name -> local_name
        self.outmap = outmap #    local_name -> outer_name

    def do(self, ctx):
        print "executing atomic action: %s" % self.name
        params = {}
        params.update(ctx)
        for name, local_name in self.inmap.iteritems():
            params[local_name] = ctx[name]

        raw_res = self.func(params)
        res = {}
        for local_name, name in self.outmap.iteritems():
            res[name] = raw_res[local_name]
            raw_res.pop(local_name)
        ctx.update(res)
        ctx.update(raw_res)
        return ctx


class SeqActions(object):
    def __init__(self, name, subtasks):
        self.kind = 'seq'
        self.name = name
        self.subtasks = subtasks


class ParActions(object):
    def __init__(self, name, subtasks):
        self.kind = 'par'
        self.name = name
        self.subtasks = subtasks


@task(name='workflow.tasks.exc_action')
def exc_action(ctx, task_obj, c_subtask):
    if task_obj.kind == 'atomic':
        res = task_obj.do(ctx)
        ctx.update(res)
        c_subtask.apply_async((ctx,))
    elif task_obj.kind == 'seq':
        print "calling exc seq"
        exc_sequence.s(ctx, task_obj, 0, c_subtask).apply_async()
    elif task_obj.kind == 'par':
        print "calling exc par"
        exc_par.s(ctx, task_obj, c_subtask).apply_async()
    else:
        print "Shouldn't be there"


@task(name='workflow.tasks.exc_sequence')
def exc_sequence(ctx, seq_actions, num, c_subtask):
    # TODO: update context after each subtask
    print "enter exc_sequence, name %s, num %s" % (seq_actions.name, num)
    if len(seq_actions.subtasks) == num:
        c_subtask.apply_async((ctx, ))
    else:
        next_in_chain = exc_sequence.s(seq_actions, num + 1, c_subtask)
        exc_action.s(ctx, seq_actions.subtasks[num], next_in_chain).apply_async()


@task(name='workflow.tasks.set_exp_status')
def set_exp_status(ctx):
    r = get_redis_instance()

    new_status = ctx.get('exp_status', 'done')
    exp = Experiment.objects.get(e_id = ctx['exp_id'])
    exp.status = new_status

    exp.save()
    #TODO: split into two functions or change name
    key_context = ExpKeys.get_context_store_key(ctx['exp_id'])
    exp.update_ctx(ctx, r)

    r.sadd(ExpKeys.get_all_exp_keys_key(ctx['exp_id']), key_context)
    print "SET_EXP_STATUS"
    print ctx


@task(name='workflow.tasks.collect_results')
def collect_results(ctx):
    exp = Experiment.objects.get(e_id = ctx['exp_id'])
    result_vars = ctx['result_vars']
    ctx['results'] = {}
    for var in result_vars:
        if var in ctx:
            ctx['results'][var] = ctx[var]

    exp.update_ctx(ctx)
    return ctx

@task(name='workflow.tasks.par_collect')
def par_collect(ctx, pre_ctx, subtask_name, parent_task):
    print "enter collect task"
    r = get_redis_instance()
    key_done = ExpKeys.get_par_done_key(ctx['exp_id'])
    key_context = ExpKeys.get_par_context_result_key(ctx['exp_id'], subtask_name)
    member = subtask_name
    r.zadd(key_done, 1, member)
    r.set(key_context, pickle.dumps(ctx))
    r.sadd(ExpKeys.get_all_exp_keys_key(ctx['exp_id']), key_done)
    r.sadd(ExpKeys.get_all_exp_keys_key(ctx['exp_id']), key_context)

    entire_zset = r.zrange(key_done, 0, -1, withscores=True)
    print entire_zset
    if len(filter(lambda x: x == 1, [v for k,v in entire_zset])) == len(parent_task.subtasks):
        ctx = {}
        ctx.update(pre_ctx)
        for st in parent_task.subtasks:
            key_context = ExpKeys.get_par_context_result_key(ctx['exp_id'], st.name)
            res_ctx = pickle.loads(r.get(key_context))
            ctx.update(res_ctx)

        key_subtask = ExpKeys.get_par_return_subtask_key(ctx['exp_id'], parent_task.name)
        c_subtask = pickle.loads(r.get(key_subtask))
        c_subtask.apply_async((ctx, ))


@task(name='workflow.tasks.exc_par')
def exc_par(ctx, par_task, c_subtask):
    print "enter exc_par, name %s" % (par_task.name,)
    r = get_redis_instance()
    key_subtask = ExpKeys.get_par_return_subtask_key(ctx['exp_id'], par_task.name)
    csbp = pickle.dumps(c_subtask)
    r.set(key_subtask, csbp )
    r.sadd(ExpKeys.get_all_exp_keys_key(ctx['exp_id']), key_subtask)
    for st in par_task.subtasks:
        cb_subtask = par_collect.s(ctx, st.name, par_task)
        exc_action.s(ctx, st, cb_subtask).apply_async()


@task(name='workflow.tasks.do_func')
def do_func(context, func):
    return func(context)

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
    at1 = AtomicAction('at:1', xadd, {1: 1, 2: 2}, {2: 'r1'})
    at2 = AtomicAction('at:2', xadd, {1: 1, 2: 2}, {2: 'r2'})
    at3 = AtomicAction('at:3', xadd, {1: 1, 2: 2}, {2: 2})


    ct = SeqActions('ct:1', [at1, at2, at3])
    ct2 = SeqActions('ct:2', [at1, ct, at2])

    pt = ParActions('pt:1', [at1, at2, ct2])

    exc_action.s({1:1, 2:2, 'exp_id': str(time.time())}, pt, xprint).apply_async()



