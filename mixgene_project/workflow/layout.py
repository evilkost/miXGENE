import time
from uuid import uuid1

from celery import task

from workflow.actions import AtomicAction, SeqActions, ParActions, exc_action, set_exp_status, collect_results
from webapp.models import UploadedData, Experiment
from workflow.common_tasks import preprocess_soft, converse_probes_to_genes
from workflow.input import CheckBoxInputVar, FileInputVar, InputGroup
from workflow.result import mixTable
from wrappers import r_test_algo, pca_test, svm_test, tt_test, mix_global_test, leukemia_data_provider


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
    def __init__(self, *args, **kwargs):
        self.input_vars = {}
        self.result_vars = []
        self.init_ctx = {}

        self.input_form = None # ! define in descendants

    def validate_exp(self, exp, request):
        """
            If all required fields are correctly configured set exp status to 'configured' and return None
            Otherwise return dict: {'field_name' -> 'error message'}
        """
        ctx = exp.get_ctx()
        has_errors = False

        if request is not None:
            fm = self.input_form(data=request.POST)
            if fm.is_valid():
                for var_name, inp_var in ctx["input_vars"].iteritems():
                    if inp_var.input_type == "group":
                        for var_name_inner, inp_var_inner in inp_var.inputs.iteritems():
                            if var_name_inner in fm.cleaned_data:
                                inp_var_inner.value = fm.cleaned_data[var_name_inner]
                            elif inp_var_inner.required:
                                has_errors = True
                                inp_var_inner.error = "This option is required"

                    if var_name in fm.cleaned_data:
                        inp_var.value = fm.cleaned_data[inp_var.name]
                    elif inp_var.required: #FIXME: what if we get empty string :)
                        has_errors = True
                        inp_var.error = "This option is required"
            else:
                has_errors = True

        return ctx, has_errors



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
        super(SampleWfL, self).__init__()
        self.template = "workflow/sample_wf.html"
        self.template_result = "workflow/sample_wf_result.html"

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


@task(name='workflow.layout.geo_fetch_dummy')
def geo_fetch_dummy(ctx):
    matrix = ctx['input_vars']['matrix']
    exp = Experiment.objects.get(e_id=ctx['exp_id'])
    result = {
        "title": "Test geo fetcher",
        "caption": "Dataset id: %s" % matrix.geo_uid,
        "filename": matrix.filename,
        "uuid": str(uuid1()),

        "has_col_names": True,
        "has_row_names": False,
        "csv_delimiter": "\t",
    }
    ctx.update({"geo_fetch_result": result})
    return ctx


class TestGeoFetcher(AbstractWorkflowLayout):
    def __init__(self):
        super(TestGeoFetcher, self).__init__()

        self.template = "workflow/test_geo_fetch.html"
        self.template_result = "workflow/test_geo_fetch_result.html"

        self.file_vars = {
            "matrix_1": "test matrix grom ncbi geo",
        }

        self.input_vars.update({
            "matrix": FileInputVar("matrix", "Matrix", "Test matrix in csv format"),
        })



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
    convert_probes_to_genes = forms.BooleanField(required=False)


class TestMultiAlgo(AbstractWorkflowLayout):
    def __init__(self):
        super(TestMultiAlgo, self).__init__()
        self.template = "workflow/test_multi_algo.html"
        self.template_result = "workflow/test_multi_algo_result.html"

        self.input_vars.update({
            "do_pca": CheckBoxInputVar("do_pca", "", "Enable PCA", is_checked=False),
            "do_global_test":  CheckBoxInputVar("do_global_test", "", "Enable Global test", is_checked=False),
            "do_linsvm": CheckBoxInputVar("do_linsvm", "", "Enable linear SVM classifier", is_checked=False),
            "do_t_test": CheckBoxInputVar("do_t_test", "", "Enable T-test", is_checked=True),

        })
        self.result_vars = ["pca_result", "mgt_result", "svm_result", "tt_result", ]

        self.init_ctx = {
            "pca_points_filename": "pca_points.csv",
            "svm_factors_filename": "linsvm_factor_vec.csv",
            "tt_test_filename": "tt_table.csv",
            "mix_global_test_filename": "mix_global_test.csv",

            "linsvm_header": ["sample #", "class" ],

            "expression_var": "leukemia_symbols",
            "phenotype_var": "leukemia_phenotype",
        }

    def validate_exp(self, exp, request):
        ctx = exp.get_ctx()
        errors = {"message": "some_errors"}
        if request is not None:
            fm = TestMultiAlgoForm(data=request.POST)
            if fm.is_valid():
                if any(fm.cleaned_data[f] for f in ["do_global_test", "do_linsvm", "do_pca", "do_t_test"]):
                    errors = None
                    for var_name, inp_var in ctx["input_vars"].iteritems():
                        if var_name in fm.cleaned_data:
                            inp_var.value = fm.cleaned_data[inp_var.name]
                else:
                    errors = {"message": "At least one test should be enabled!"}


        ctx.update()
        return (ctx, errors)

    def get_main_action(self, ctx):
        get_leukemia_data_action = AtomicAction("get_dataset", leukemia_data_provider, {}, {})

        pca_action = AtomicAction("pca_action", pca_test, {"pca_points_filename": "filename"}, {"result": "pca_result"})
        svm_action = AtomicAction("svm_action", svm_test, {"svm_factors_filename": "filename"}, {"result": "svm_result"})
        tt_action = AtomicAction("tt_action", tt_test, {"tt_test_filename": "filename"}, {"result": "tt_result"})
        mix_global_test_action = AtomicAction("mix_global_test", mix_global_test,
                                              {"mix_global_test_filename": "filename"}, {"result": "mgt_result"})

        par_actions = []

        if ctx["input_vars"]["do_global_test"].value:
            par_actions.append(mix_global_test_action)

        if ctx["input_vars"]["do_linsvm"].value:
            par_actions.append(svm_action)

        if ctx["input_vars"]["do_pca"].value:
            par_actions.append(pca_action)

        if ctx["input_vars"]["do_t_test"].value:
            par_actions.append(tt_action)

        alg_actions = ParActions("alg_action", par_actions)
        collect_res_action = AtomicAction("collect_results", collect_results, {}, {})

        return SeqActions("main_action", [
            get_leukemia_data_action,
            alg_actions,
            collect_res_action
        ])

class TestMultiAlgo2(AbstractWorkflowLayout):
    def __init__(self):
        super(TestMultiAlgo2, self).__init__()
        self.template = "workflow/test_multi_algo.html"
        self.template_result = "workflow/test_multi_algo_result.html"

        self.input_form = TestMultiAlgoForm
        self.input_vars.update({
            "common_settings": InputGroup("common", "Settings", "", inputs={
                "convert_probes_to_genes": CheckBoxInputVar("convert_probes_to_genes", "",
                    "Convert Probe ID to ENTREZ_GENE_ID and operate with genes as units"),
            }),
            "algo_switch": InputGroup("algo_switch", "Choose plugins", "", inputs={
                "do_pca": CheckBoxInputVar("do_pca", "", "Enable PCA", is_checked=False),
                "do_global_test":  CheckBoxInputVar("do_global_test", "", "Enable Global test", is_checked=False),
                "do_linsvm": CheckBoxInputVar("do_linsvm", "", "Enable linear SVM classifier", is_checked=False),
                "do_t_test": CheckBoxInputVar("do_t_test", "", "Enable T-test", is_checked=True),
            }),

            "dataset": FileInputVar("dataset", "Dataset", "Test dataset, please provide file in SOFT format")
        })
        self.result_vars = ["pca_result", "mgt_result", "svm_result", "tt_result", ]

        self.init_ctx = {
            "pca_points_filename": "pca_points.csv",
            "svm_factors_filename": "linsvm_factor_vec.csv",
            "tt_test_filename": "tt_table.csv",
            "mix_global_test_filename": "mix_global_test.csv",

            "linsvm_header": ["sample #", "class" ],

            "expression_var": "expression",
            "expression_trans_var": "expression",
            "phenotype_var": "phenotype",
            "gene_sets_var": "gene_sets",

            "dataset_var": "dataset",
        }

    def validate_exp(self, exp, request):
        ctx, has_errors = super(TestMultiAlgo2, self).validate_exp(exp, request)

        # idea
        #errors = [] # keys of errors
        #errors_messages = {} # errors messages

        if request is not None:
            fm = TestMultiAlgoForm(data=request.POST)
            if fm.is_valid():
                if not any(fm.cleaned_data[f] for f in ["do_global_test", "do_linsvm", "do_pca", "do_t_test"]):
                    has_errors = True
                    self.input_vars["algo_switch"].error = "At least one test should be enabled!"

            #TODO: other specific checks

        if ctx["dataset_var"] in ctx["input_vars"]:
            print "is_done: ", ctx["input_vars"][ctx["dataset_var"]].is_done
            if not ctx["input_vars"][ctx["dataset_var"]].is_done:
                has_errors = True

        if has_errors:
            errors = {"foo": "bar"}
        else:
            errors = None

        if ctx["input_vars"]["common_settings"].inputs["convert_probes_to_genes"]:
            ctx["units"] = "genes"
        else:
            ctx["units"] = "probes"

        return ctx, errors

    def get_main_action(self, ctx):
        main_sequence = []
        prepare_dataset = AtomicAction("prepare_dataset", preprocess_soft, {}, {})
        main_sequence.append(prepare_dataset)

        if ctx["input_vars"]["common_settings"].inputs["convert_probes_to_genes"]:
            convers_probes = AtomicAction("convers_probes", converse_probes_to_genes, {}, {})
            main_sequence.append(convers_probes)

        pca_action = AtomicAction("pca_action", pca_test, {"pca_points_filename": "filename"}, {"result": "pca_result"})
        svm_action = AtomicAction("svm_action", svm_test, {"svm_factors_filename": "filename"}, {"result": "svm_result"})
        tt_action = AtomicAction("tt_action", tt_test, {"tt_test_filename": "filename"}, {"result": "tt_result"})
        mix_global_test_action = AtomicAction("mix_global_test", mix_global_test,
                                              {"mix_global_test_filename": "filename"}, {"result": "mgt_result"})

        par_actions = []

        if ctx["input_vars"]["algo_switch"].inputs["do_global_test"].value and ctx["units"] == "genes":
            par_actions.append(mix_global_test_action)

        if ctx["input_vars"]["algo_switch"].inputs["do_linsvm"].value:
            par_actions.append(svm_action)

        if ctx["input_vars"]["algo_switch"].inputs["do_pca"].value:
            par_actions.append(pca_action)

        if ctx["input_vars"]["algo_switch"].inputs["do_t_test"].value:
            par_actions.append(tt_action)

        alg_actions = ParActions("alg_action", par_actions)
        collect_res_action = AtomicAction("collect_results", collect_results, {}, {})

        main_sequence.append(alg_actions)
        main_sequence.append(collect_res_action)

        return SeqActions("main_action", main_sequence)
