from celery import task
from celery import chain

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

class ChainTask(object):
    def __init__(self, name, subtasks):
        self.kind = 'chain'
        self.name = name
        self.subtasks = subtasks
        self.on_finish = on_finish

@task(name='workflow.tasks.exc_task')
def exc_task(ctx, task_obj, c_subtask):
    if task_obj.kind == 'atomic':
        res = task_obj.do(ctx)
        ctx.update(res)
        c_subtask.apply_async((ctx,))
    elif task_obj.kind == 'chain':
        print "callin exc chain"
        exc_chain.s(ctx, task_obj, 0, c_subtask).apply_async()


@task(name='worflow.tasks.exc_chain')
def exc_chain(ctx, tasks_chain, num, c_subtask):
    print "enter exc_chain, name %s, num %s" % (tasks_chain.name, num)
    if len(tasks_chain.subtasks) == num:
        c_subtask.apply_async((ctx,))
    else:
        next_in_chain = exc_chain.s(tasks_chain, num + 1, c_subtask)
        exc_task.s(ctx, tasks_chain.subtasks[num], next_in_chain).apply_async()

#TODO: Chord task implementation


### Usage example
@task(name='workflow.tasks.xprint')
def xprint(context):
    print "xprint context: %s" % context


def xadd(context):
    ctx = {}
    ctx.update(context)
    #ctx['a'] = context['a'] + context['b']
    ctx[2] = context[1] + context[2]
    print 'xadd context', ctx
    return ctx

def usage_expample():
    """
    at1 = AtomicTask('at:1', xadd, {1: 'a', 2: 'b'}, {'a': 3})
    at2 = AtomicTask('at:2', xadd, {1: 'a', 2: 'b'}, {'a': 3})
    at3 = AtomicTask('at:3', xadd, {1: 'a', 2: 'b'}, {'a': 3})
    """
    at1 = AtomicTask('at:1', xadd, {1: 1, 2: 2}, {2: 2})
    at2 = AtomicTask('at:2', xadd, {1: 1, 2: 2}, {2: 2})
    at3 = AtomicTask('at:3', xadd, {1: 1, 2: 2}, {2: 2})


    ct = ChainTask('ct:1', [at1, at2, at3], xprint.s())
    ct2 = ChainTask('ct:2', [at1, ct, at2], xprint.s())
    exc_task.s({1:1, 2:2}, ct2, xprint).apply_async()
