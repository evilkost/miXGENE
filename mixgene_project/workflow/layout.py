from celery import task

from workflow.tasks import AtomicTask, SeqTask, ParTask, exc_task
from mixgene.util import get_redis_instance

import time

@task(name='workflow.layout.wait_task')
def wait_task(ctx):
    t = ctx.get('sleep_time', 0)
    print "sleep for %s" % t
    time.sleep(t)
    ctx['sleep_done'] = 1
    return ctx


@task(name='workflow.layout.write_result')
def write_result(ctx):
    r = get_redis_instance()
    print ctx['exp_id']
    r.set("SAMPLE_WORKFLOW_LAYOUT:exp_id=%s:result" % ctx['exp_id'], "DONE")

class SampleWfL(object):
    """
        TODO: Or generate from json DSL
    """

    def __init__(self):
        self.description = """Sample Workflow Layout

        using seq and par subtasks
                    |- seq task [-- at1 --- at2 --] --|
        --par task--|                                 |--- finish
                    |------------at3------------------|
        """
        self.template = "workflow/sample_wf.html"
        self.template_result = "workflow/sample_wf_result.html"

    def get_workflow(self, request):
        at1 = AtomicTask("at_1", wait_task, {'t1': 'sleep_time'}, {})
        at2 = AtomicTask("at_2", wait_task, {'t2': 'sleep_time'}, {})
        at3 = AtomicTask("at_3", wait_task, {'t3': 'sleep_time'}, {})

        seqt = SeqTask("seqt", [at1, at2])
        main_task = ParTask("part", [seqt, at3])


        #TODO: dedicated method to parse request -> context, maybe
        ctx = {}
        ctx['t1'] = int(request.POST.get('t1'))
        ctx['t2'] = int(request.POST.get('t2'))
        ctx['t3'] = int(request.POST.get('t3'))

        return (main_task, ctx)
        #return exc_task.s(ctx, main_task, write_result)
