import time
from celery import task

from workflow.actions import AtomicAction, SeqActions, ParActions, exc_action, set_exp_status
from webapp.models import Experiment, UploadedData

@task(name='workflow.layout.wait_task')
def wait_task(ctx):
    t = ctx.get('sleep_time', 0)
    print "sleep for %s" % t
    time.sleep(t)
    ctx['sleep_done'] = 1
    return ctx


@task(name='workflow.layout.write_result')
def write_result(ctx):
    print ctx['exp_id']

#TODO: Or generate from json DSL

class AbstractWorkflowLayout(object):
    """
        Sceletone to create custom workflows
    """
    def __init_(self):
        pass

    def validate_ctx(self, exp, request):
        """
            If all required fields are correctly configured set exp status to 'configured' and return None
            Otherwise return dict: {'field_name' -> 'error message'}
        """
        pass

    def run_experiment(self, exp):
        """
            Run experiment if it was previously correctly configured
        """
        if exp.status == "configured":
            ctx = exp.get_ctx()
            main_action = self.get_main_action(ctx)
            exc_action.s(ctx, main_action, set_exp_status).apply_async()
            exp.status = "running"
            exp.save()
        else:
            print "error: exp isn't in configured state"

    def on_delete(self, experiment):
        pass


from django import forms

class SampleWfLForm(forms.Form):
    t1 = forms.IntegerField(min_value=0, max_value=10)
    t2 = forms.IntegerField(min_value=0, max_value=10)
    t3 = forms.IntegerField(min_value=0, max_value=10)

class SampleWfL(AbstractWorkflowLayout):
    """
        using seq and par subtasks
                    |- seq task [-- at1 ->- at2 --] --|
        ->-par task-|                                 |->- finish
                    |----->------at3--------->--------|
    """

    def __init__(self):
        self.template = "workflow/sample_wf.html"
        self.template_result = "workflow/sample_wf_result.html"
        self.data_files_vars = []

    def validate_exp(self, exp, request):
        ctx = exp.get_ctx()
        fm = SampleWfLForm(data=request.POST)
        if fm.is_valid():
            errors = None
        else:
            errors = {"message": "some_errors"}
        ctx.update(fm.cleaned_data)
        return (ctx, errors)

    def get_main_action(self, ctx):
        at1 = AtomicAction("at_1", wait_task, {'t1': 'sleep_time'}, {})
        at2 = AtomicAction("at_2", wait_task, {'t2': 'sleep_time'}, {})
        at3 = AtomicAction("at_3", wait_task, {'t3': 'sleep_time'}, {})
        seqt = SeqActions("seqt", [at1, at2])
        main_action = ParActions("part", [seqt, at3])
        return main_action


@task(name='workflow.layout.r_test_algo')
def r_test_algo(ctx):
    import  rpy2.robjects as R
    from rpy2.robjects.packages import importr
    test = importr("test")
    from webapp.models import Experiment, UploadedData
    exp = Experiment.objects.get(e_id = ctx['exp_id'])
    ud = UploadedData.objects.get(exp=exp, var_name="data.csv")
    filename = ud.data.file.name

    rread_csv = R.r['read.csv']
    rwrite_csv = R.r['write.csv']
    rtest = R.r['test']

    rx = rread_csv(filename)
    rres = rtest(rx)

    names_to_res = ['sum', 'nrow', 'ncol',]
    for i in range(len(rres.names)):
        if rres.names[i] in names_to_res:
            ctx[rres.names[i]] = rres[i][0]

    return ctx


class TestRAlgo(AbstractWorkflowLayout):
    def __init__(self):
        self.template = "workflow/test_r_wf.html"
        self.template_result = "workflow/test_r_result.html"
        self.data_files_vars = [
            u"data.csv",
        ]

    def get_main_action(self, ctx):
        return AtomicAction("rtest", r_test_algo, {}, {})

    def validate_exp(self, exp, request):
        #import ipdb; ipdb.set_trace()
        uploaded_name = [x.var_name for x in UploadedData.objects.filter(exp=exp)]
        errors = None
        if all(var in uploaded_name for var in self.data_files_vars):
            uploads_done = True
        else:
            uploads_done = False
            errors = {"message": "Data not uploaded"}

        return (exp.get_ctx(), errors)

    """
    def get_workflow(self, request):
        ctx = {}
        main_task = AtomicAction("rtest", r_test_algo, {}, {})
        return (main_task, ctx)
    """
