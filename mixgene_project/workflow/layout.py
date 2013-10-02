import time
from celery import task

from workflow.actions import AtomicAction, SeqActions, ParActions, exc_action, set_exp_status, collect_results
from webapp.models import Experiment, UploadedData
from wrappers import r_test_algo, pca_test, svm_test, tt_test, mix_global_test

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
    exp = Experiment.objects.get(e_id=ctx['exp_id'])
    ctx = exp.get_ctx()

    filename = ctx["data.csv"]

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
        ctx_filenames = {}
        if all(var in uploaded_name for var in self.data_files_vars):
            uploads_done = True
            for x in UploadedData.objects.filter(exp=exp):
                ctx_filenames[x.var_name] = x.data.file.name
            exp.update_ctx(ctx_filenames)
        else:
            uploads_done = False
            errors = {"message": "Data not uploaded"}
        return (exp.get_ctx(), errors)


@task(name='workflow.layout.geo_fetch_dummy')
def geo_fetch_dummy(ctx):
    matrix = ctx['exp_file_vars']['matrix']
    result = {
        "caption": "Dataset id: %s" % matrix.geo_uid,
        "filename": matrix.filename,
        #"filepath":

        "has_col_names": True,
        "has_row_names": False,
        "csv_delimiter": "\t",
    }
    ctx.update({"geo_fetch_result": result})
    return ctx



class TestGeoFetcher(AbstractWorkflowLayout):
    def __init__(self):
        self.template = "workflow/test_geo_fetch.html"
        self.template_result = "workflow/test_geo_fetch_result.html"
        self.data_files_vars = []

        self.file_vars = {
            "matrix": "test matrix grom ncbi geo",
        }

    def get_main_action(self, ctx):
        return AtomicAction("geo_fetch_dummy", geo_fetch_dummy, {}, {})

    def validate_exp(self, exp, request):
        ctx = exp.get_ctx()

        file_vars_fetched = ctx['exp_file_vars'].keys()
        if all([f in self.file_vars.keys() for f in file_vars_fetched]):
            errors = None
        else:
            errors = {"message": "Data was not uploaded"}
        return (exp.get_ctx(), errors)


class TestMultiAlgoForm(forms.Form):
    #samples_num = forms.IntegerField(min_value=1, max_value=150)
    do_global_test = forms.BooleanField(required=False)
    do_linsvm = forms.BooleanField(required=False)
    do_pca = forms.BooleanField(required=False)
    do_t_test = forms.BooleanField(required=False)


class AbsInputVar(object):
    def __init__(self, name, title, description, *args, **kwargs):
        self.name = name
        self.title = title
        self.description = description


class CheckBoxInputVar(AbsInputVar):
    def __init__(self, *args, **kwargs):
        super(CheckBoxInputVar, self).__init__(*args, **kwargs)
        self.input_type = "checkbox"
        self.value = kwargs.get('is_checked', True)


class TestMultiAlgo(AbstractWorkflowLayout):
    def __init__(self):
        self.template = "workflow/test_multi_algo.html"
        self.template_result = "workflow/test_multi_algo_result.html"
        self.data_files_vars = []

        self.input_vars = [
            CheckBoxInputVar("do_pca", "", "Enable PCA", is_checked=False),
            CheckBoxInputVar("do_global_test", "", "Enable Global test", is_checked=False),
            CheckBoxInputVar("do_linsvm", "", "Enable linear SVM classifier", is_checked=False),
            CheckBoxInputVar("do_t_test", "", "Enable T-test", is_checked=True),
        ]
        self.result_vars = ["pca_result", "mgt_result", "svm_result", "tt_result", ]

    def validate_exp(self, exp, request):
        ctx = exp.get_ctx()
        fm = TestMultiAlgoForm(data=request.POST)
        if fm.is_valid():
            if any(fm.cleaned_data[f] for f in ["do_global_test", "do_linsvm", "do_pca", "do_t_test"]):
                errors = None
                for inp_var in ctx["input_vars"]:
                    if inp_var.name in fm.cleaned_data:
                        inp_var.value = fm.cleaned_data[inp_var.name]
            else:
                errors = {"message": "At least one test should be enabled!"}
        else:
            errors = {"message": "some_errors"}

        ctx.update({
            "pca_points_filename": "pca_points.csv",
            "svm_factors_filename": "linsvm_factor_vec.csv",
            "tt_test_filename": "tt_table.csv",
            "mix_global_test_filename": "mix_global_test.csv",

            "linsvm_header": ["sample #", "class" ],
        })
        return (ctx, errors)

    def get_main_action(self, ctx):
        pca_action = AtomicAction("pca_action", pca_test, {"pca_points_filename": "filename"}, {"result": "pca_result"})
        svm_action = AtomicAction("svm_action", svm_test, {"svm_factors_filename": "filename"}, {"result": "svm_result"})
        tt_action = AtomicAction("tt_action", tt_test, {"tt_test_filename": "filename"}, {"result": "tt_result"})

        mix_global_test_action = AtomicAction("mix_global_test", mix_global_test,
                                              {"mix_global_test_filename": "filename"}, {"result": "mgt_result"})

        par_actions =[]
        for inp_var in ctx["input_vars"]:
            if inp_var.name == "do_global_test" and inp_var.value:
                par_actions.append(mix_global_test_action)
            if inp_var.name == "do_linsvm" and inp_var.value:
                par_actions.append(svm_action)
            if inp_var.name == "do_pca" and inp_var.value:
                par_actions.append(pca_action)
            if inp_var.name == "do_t_test" and inp_var.value:
                par_actions.append(tt_action)



        alg_actions = ParActions("alg_action", par_actions)

        collect_res_action = AtomicAction("collect_results", collect_results, {}, {})
        return SeqActions("main_action", [alg_actions, collect_res_action])
