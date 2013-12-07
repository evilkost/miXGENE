from collections import defaultdict
import json
from uuid import uuid1

from django import forms
from django.forms import ModelForm
from fysom import Fysom
import pandas as pd
from webapp.models import Experiment, BroadInstituteGeneSet

from workflow.common_tasks import fetch_geo_gse, preprocess_soft

from workflow.structures import ExpressionSet
from workflow import wrappers


class GenericBlock(object):
    block_base_name = "GENERIC_BLOCK"
    provided_objects = {}
    provided_objects_inner = {}
    create_new_scope = False
    sub_scope = None
    is_base_name_visible = True

    def __init__(self, name, type, exp_id):
        """
            Building block for workflow
            @type can be: "user_input", "computation"
        """
        self.uuid = uuid1().hex
        self.name = name
        self.type = type
        self.exp_id = exp_id

        # pairs of (var name, data type, default name in context)
        self.required_inputs = []
        self.provide_outputs = []

        self.state = "created"

        self.errors = []
        self.warnings = []
        self.base_name = ""

        self.scope = "root"

    def clean_errors(self):
        self.errors = []

    def get_available_user_action(self):
        return self.get_allowed_actions(True)

    def get_allowed_actions(self, only_user_actions=False):
        action_list = []
        for line in self.all_actions:
            # action_code, action_title, user_visible = line

            action_code = line[0]
            user_visible = line[2]
            self.fsm.current = self.state

            if self.fsm.can(action_code) and \
                    (not only_user_actions or user_visible):
                action_list.append(line)
        return action_list

    def do_action(self, action_name, *args, **kwargs):
        #TODO: add notification to html client
        if action_name in [row[0] for row in self.get_allowed_actions()]:
            self.fsm.current = self.state
            getattr(self.fsm, action_name)()
            self.state = self.fsm.current
            print "change state to: " + self.state

            getattr(self, action_name)(*args, **kwargs)
        else:
            raise RuntimeError("Action %s isn't available" % action_name)

    def before_render(self, exp, *args, **kwargs):
        """
        Invoke prior to template applying, prepare relevant data
        @param exp: Experiment
        @return: additional content for template context
        """
        return {}

    def save_form(self, exp, request, *args, **kwargs):
        self.form = self.form_cls(request.POST)
        self.validate_form()
        exp.store_block(self)

    def validate_form(self):
        if self.form.is_valid():
            #TODO: additional checks e.g. other blocks doesn't provide
            #      variables with the same names
            self.do_action("on_form_is_valid")
        else:
            self.do_action("on_form_not_valid")

    def serialize_to_dict(self, exp):
        self.before_render(exp)
        keys_to_snatch = set([
            "uuid", "base_name", "name", "type",
            "errors", "warnings"
        ])
        hash = {}
        for key in keys_to_snatch:
            hash[key] = getattr(self, key)
        return hash

    #def to_json(self):
    #    return json.dumps(self.serialize_to_dict())


class GeneSetSelectionForm(forms.Form):
    gene_set_id = forms.IntegerField()

    def clean_gene_set_id(self):
        data = self.cleaned_data['gene_set_id']
        if len(BroadInstituteGeneSet.objects.filter(id=data)) == 0 :
            raise forms.ValidationError("Got wrong gene set identifier, try again")


class GetBroadInstituteGeneSet(GenericBlock):
    fsm = Fysom({
        'events': [
        {'name': 'save_form', 'src': 'created', 'dst': 'form_modified'},
        {'name': 'save_form', 'src': 'form_modified', 'dst': 'form_modified'},
        {'name': 'save_form', 'src': 'form_valid', 'dst': 'form_modified'},

        {'name': 'on_form_is_valid', 'src': 'form_modified', 'dst': 'form_valid'},
        {'name': 'on_form_not_valid', 'src': 'form_modified', 'dst': 'form_modified'},
    ]})

    all_actions = [
        # method name, human readable title, user visible
        ("save_form", "Select", True),

        ("on_form_is_valid", "", False),
        ("on_form_not_valid", "", False),
    ]

    widget = "widgets/get_bi_gene_set.html"
    form_cls = GeneSetSelectionForm
    block_base_name = "BI_GENE_SET"
    is_base_name_visible = True

    provided_objects = {
        "gene_sets": "GeneSets",
    }

    def __init__(self, *args, **kwargs):
        super(GetBroadInstituteGeneSet, self).__init__("Get MSigDB gene set", "user_input",  *args, **kwargs)
        self.gmt = None
        self.errors = []
        self.form = None
        self.selected_gs_id = None

    def before_render(self, exp, *args, **kwargs):
        self.all_gene_sets = BroadInstituteGeneSet.objects.order_by("section", "name", "unit")
        return {}

    def on_form_is_valid(self):
        self.errors = []
        self.selected_gs_id = int(self.form["gene_set_id"].value())
        print self.selected_gs_id

    def on_form_not_valid(self):
        pass


class FetchGseForm(forms.Form):
    # Add custom validator to check GSE prefix
    #  If file is fetched or is being fetched don't allow to change geo_uid
    geo_uid = forms.CharField(min_length=4, max_length=31, required=True)
    # expression_set_name = forms.CharField(label="Name for expression set",
    #                                       max_length=255)
    # gpl_annotation_name = forms.CharField(label="Name for GPL annotation",
    #                                       max_length=255)

    def clean_geo_uid(self):
        data = self.cleaned_data['geo_uid']
        if data[:3].upper() != 'GSE' not in data:
            raise forms.ValidationError("Geo uid should have 'GSE' prefix")

        # Always return the cleaned data, whether you have changed it or
        # not.
        return data


class FetchGSE(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'save_form', 'src': 'created', 'dst': 'form_modified'},
            {'name': 'save_form', 'src': 'form_modified', 'dst': 'form_modified'},

            {'name': 'on_form_is_valid', 'src': 'form_modified', 'dst': 'form_valid'},
            {'name': 'on_form_not_valid', 'src': 'form_modified', 'dst': 'form_modified'},

            {'name': 'reset_form', 'src': 'form_modified', 'dst': 'created'},
            {'name': 'reset_form', 'src': 'form_valid', 'dst': 'created'},

            {'name': 'show_form', 'src': 'form_valid', 'dst': 'form_modified'},

            {'name': 'start_fetch', 'src': 'form_valid', 'dst': 'source_is_being_fetched'},

            {'name': 'error_during_fetch', 'src': 'source_is_being_fetched', 'dst': 'form_valid'},
            {'name': 'successful_fetch', 'src': 'source_is_being_fetched', 'dst': 'source_was_fetched'},

            {'name': 'start_preprocess', 'src': 'source_was_fetched', 'dst': 'source_is_being_preprocessed'},

            {'name': 'error_during_preprocess', 'src': 'source_is_being_preprocessed', 'dst': 'form_valid'},
            {'name': 'successful_preprocess', 'src': 'source_is_being_preprocessed', 'dst': 'source_was_preprocessed'},

            {'name': 'assign_sample_classes', 'src': 'source_was_preprocessed', 'dst': 'sample_classes_assigned'},
            {'name': 'assign_sample_classes', 'src': 'sample_classes_assigned', 'dst': 'sample_classes_assigned'},
        ],
    })

    all_actions = [
        # method name, human readable title, user visible
        ("save_form", "Save parameters", True),

        ("on_form_is_valid", "", False),
        ("on_form_not_valid", "", False),

        ("show_form", "Edit parameters", True),
        ("reset_form", "Reset parameters", True),

        ("start_fetch", "Fetch data", True),
        ("error_during_fetch", "", False),
        ("successful_fetch", "", False),

        ("start_preprocess", "", False),
        ("error_during_preprocess", "", False),
        ("successful_preprocess", "", False),

        ("assign_sample_classes", "", False),
    ]
    widget = "widgets/fetch_ncbi_gse.html"
    pages = {
        "assign_sample_classes": "widgets/fetch_gse/assign_sample_classes.html",
    }
    form_cls = FetchGseForm
    form_data = {
        "expression_set_name": "expression",
        "gpl_annotation_name": "annotation",
    }
    block_base_name = "FETCH_GEO"
    is_base_name_visible = True

    provided_objects =  {
        "expression_set": "ExpressionSet",
        "annotation": "PlatformAnnotation",
    }

    def __init__(self, *args, **kwargs):
        super(FetchGSE, self).__init__("Fetch ncbi gse", "user_input",  *args, **kwargs)


        self.form = self.form_cls(self.form_data)

        self.source_file = None

        self.celery_task_fetch = None
        self.celery_task_preprocess = None

        self.errors = []

        self.expression_set = None
        self.gpl_annotation = None

    def get_expression_set_name(self):
        return self.form["expression_set_name"].value()

    def get_gse_source_name(self):
        return "%s_source" % self.uuid

    def get_geo_uid(self):
        return self.form["geo_uid"].value()

    def get_annotation_name(self):
        return self.form["gpl_annotation_name"].value()

    def is_form_fields_editable(self):
        if self.state in ['created', 'form_modified']:
            return True
        return False

    def is_sub_pages_visible(self):
        if self.state in ['source_was_preprocessed', 'sample_classes_assigned']:
            return True
        return False

    def on_form_is_valid(self):
        self.errors = []

    def on_form_not_valid(self):
        pass

    def show_form(self, exp, *args, **kwargs):
        exp.store_block(self)

    def reset_form(self, exp, *args, **kwargs):
        self.clean_errors()
        self.form = self.form_cls(self.form_data)
        self.is_form_visible = True
        self.file_can_be_fetched = False
        exp.store_block(self)

    def start_fetch(self, exp, *args, **kwargs):
        self.clean_errors()
        self.celery_task_fetch = fetch_geo_gse.s(exp, self, ignore_cache=False).apply_async()
        exp.store_block(self)

    def error_during_fetch(self, exp, *args, **kwargs):
        exp.store_block(self)

    def successful_fetch(self, exp, *args, **kwargs):
        self.clean_errors()
        self.do_action("start_preprocess", exp)
        exp.store_block(self)

    def start_preprocess(self, exp, *args, **kwargs):
        self.celery_task_preprocess = preprocess_soft.s(exp, self).apply_async()
        exp.store_block(self)

    def error_during_preprocess(self, exp, *args, **kwargs):
        exp.store_block(self)

    def successful_preprocess(self, exp, *args, **kwargs):
        self.clean_errors()
        exp.store_block(self)

    def assign_sample_classes(self, exp, request, *args, **kwargs):
        pheno_df = self.expression_set.get_pheno_data_frame()
        sample_classes = json.loads(request.POST['sample_classes'])
        pheno_df['User_class'] = pd.Series(sample_classes)

        self.expression_set.store_pheno_data_frame(pheno_df)
        exp.store_block(self)

    def revoke_task(self, exp, *args, **kwargs):
        pass
        #self.celery_task_fetch.revoke(terminate=True)
        #exp.store_block(self)


def prepare_bound_variable_select_input(available, block_aliases_map, block_name, field_name):
    """
    @type  available: [(block_uuid, var_name),]
    @param available: list of available variables

    @type  block_aliases_map: dict
    @param block_aliases_map: Block uuid -> block alias

    @type  block_name: str
    @param block_name: Current bound variable parent block

    @type  field_name: str
    @param field_name: Current bound variable name

    @rtype: list of [(uuid, block_name, field_name, ?is_selected)]
    @return: prepared list for select input
    """
    marked = []
    for uuid, i_field_name in available:
        i_block_name = block_aliases_map[uuid]
        if i_block_name == block_name and i_field_name == field_name:
            marked.append((uuid, i_block_name, i_field_name, True))
        else:
            marked.append((uuid, i_block_name, i_field_name, False))
    return marked


class CrossValidation(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variable', 'src': 'created', 'dst': 'variable_bound'},
            {'name': 'bind_variable', 'src': 'finished', 'dst': 'variable_bound'},
            {'name': 'bind_variable', 'src': 'variable_bound', 'dst': 'variable_bound'},

            {'name': 'split_dataset', 'src': 'variable_bound', 'dst': 'split_dataset'},

            {'name': 'run_sub_blocks', 'src': 'split_dataset', 'dst': 'split_dataset'},
            {'name': 'run_sub_blocks', 'src': 'split_dataset', 'dst': 'finished'},

        ]
    })
    widget = "widgets/cross_validation_base.html"
    block_base_name = "CROSS_VALID"
    all_actions = [
        ("bind_variable", "Select variable", True),

        ("success", "", False),
        ("error", "", False)

    ]
    create_new_scope = True

    provided_objects = {}
    provided_objects_inner = {
        "es_train_i": "ExpressionSet",
        "es_test_i": "ExpressionSet",
    }


    @property
    def sub_blocks(self):
        uuids_blocks = Experiment.get_blocks(self.children_blocks)
        exp = Experiment.objects.get(e_id = self.exp_id)
        for uuid, block in uuids_blocks:
            block.before_render(exp)

        return uuids_blocks


    @property
    def sub_scope(self):
        return "%s_%s" % (self.scope, self.uuid)

    def __init__(self, *args, **kwargs):
        super(CrossValidation, self).__init__("Cross Validation", "Meta", *args, **kwargs)
        self.bound_variable_field = None
        self.bound_variable_block = None
        self.bound_variable_block_alias = None

        self.children_blocks = []

        self.active_fold = -1
        self.fold_number = 10
        self.train_test_ratio = 0.7

    ### inner variables provider
    @property
    def es_train_i(self):
        return None

    @property
    def es_test_i(self):
        return None

    ### end inner variables
    def bind_variable(self, exp, request, *args, **kwargs):
        split = request.POST['variable_name'].split(":")
        self.bound_variable_block = split[0]
        bound_block = exp.get_block(self.bound_variable_block)
        self.bound_variable_block_alias = bound_block.base_name
        self.bound_variable_field = ''.join(split[1:])
        exp.store_block(self)

    def before_render(self, exp, *args, **kwargs):
        context_add = {}
        available = exp.get_visible_variables(scopes=[self.scope], data_types=["ExpressionSet"])

        self.variable_options = prepare_bound_variable_select_input(
            available, exp.get_block_aliases_map(),
            self.bound_variable_block_alias, self.bound_variable_field)

        if len(self.variable_options) == 0:
            self.errors.append(Exception("There is no blocks which provides Expression Set"))

        return context_add


class PCA_visualize(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variable', 'src': 'created', 'dst': 'variable_bound'},
            {'name': 'bind_variable', 'src': 'pca_computed', 'dst': 'variable_bound'},
            {'name': 'bind_variable', 'src': 'variable_bound', 'dst': 'variable_bound'},

            {'name': 'run_pca', 'src': 'variable_bound', 'dst': 'pca_computing'},

            {'name': 'success', 'src': 'pca_computing', 'dst': 'pca_computed'},
            {'name': 'error', 'src': 'pca_computing', 'dst': 'variable_bound'},
        ]
    })

    widget = "widgets/pca_view.html"
    block_base_name = "PCA_VIEW"
    all_actions = [
        ("bind_variable", "Select variable", True),
        ("run_pca", "Run PCA", True),

        ("success", "", False),
        ("error", "", False)
    ]

    def __init__(self, *args, **kwargs):
        super(PCA_visualize, self).__init__("PCA Analysis", "Visualisation", *args, **kwargs)
        self.bound_variable_field = None
        self.bound_variable_block = None
        self.bound_variable_block_alias = None

        self.pca_result = None
        self.celery_task = None

    @property
    def pca_result_in_json(self):
        df = self.pca_result.get_pca()
        return df.to_json(orient="split")

    def run_pca(self, exp, request, *args, **kwargs):
        self.clean_errors()
        bound_block = Experiment.get_block(self.bound_variable_block)
        es = getattr(bound_block, self.bound_variable_field)
        self.celery_task = wrappers.pca_test.s(exp, self, es)
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp):
        exp.store_block(self)

    def error(self, exp):
        exp.store_block(self)

    def bind_variable(self, exp, request, *args, **kwargs):
        split = request.POST['variable_name'].split(":")
        self.bound_variable_block = split[0]
        bound_block = exp.get_block(self.bound_variable_block)
        self.bound_variable_block_alias = bound_block.base_name
        self.bound_variable_field = ''.join(split[1:])
        exp.store_block(self)

    def before_render(self, exp, *args, **kwargs):
        context_add = {}
        available = exp.get_visible_variables(scopes=[self.scope], data_types=["ExpressionSet"])

        self.variable_options = prepare_bound_variable_select_input(
            available, exp.get_block_aliases_map(),
            self.bound_variable_block_alias, self.bound_variable_field)

        if len(self.variable_options) == 0:
            self.errors.append(Exception("There is no blocks which provides Expression Set"))

        return context_add


class ExpressionSetDetails(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variable', 'src': 'created', 'dst': 'variable_bound'},
            {'name': 'bind_variable', 'src': 'variable_bound', 'dst': 'variable_bound'},
        ]
    })

    widget = "widgets/expression_set_view.html"
    block_base_name = "ES_VIEW"
    all_actions = [
        ("bind_variable", "Select expression set", True)
    ]

    def __init__(self, *args, **kwargs):
        super(ExpressionSetDetails, self).__init__("Expression set details", "Visualisation", *args, **kwargs)
        self.bound_variable_field = None
        self.bound_variable_block = None
        self.bound_variable_block_alias = None

        self.variable_options = []

    def bind_variable(self, exp, request, *args, **kwargs):
        self.clean_errors()
        split = request.POST['variable_name'].split(":")
        self.bound_variable_block = split[0]
        bound_block = exp.get_block(self.bound_variable_block)
        self.bound_variable_block_alias = bound_block.base_name
        self.bound_variable_field = ''.join(split[1:])
        exp.store_block(self)

    def before_render(self, exp, *args, **kwargs):
        context_add = {}

        #import ipdb; ipdb.set_trace()
        available = exp.get_visible_variables(scopes=[self.scope], data_types=["ExpressionSet"])
        self.variable_options = prepare_bound_variable_select_input(
            available, exp.get_block_aliases_map(),
            self.bound_variable_block_alias, self.bound_variable_field)

        if len(self.variable_options) == 0:
            self.errors.append(Exception("There is no blocks which provides Expression Set"))
        #return {"variable_options": available["ExpressionSet"]}

        if self.state == "variable_bound":
            bound_block = Experiment.get_block(self.bound_variable_block)
            #import ipdb; ipdb.set_trace()
            if not isinstance(getattr(bound_block, self.bound_variable_field), ExpressionSet):
                self.errors.append(Exception("Bound variable isn't ready"))


        self.errors.append({
            "msg": "Not implemented !"
        })
        return context_add

    def get_es_preview(self):
        if self.state != "variable_bound":
            return ""
        bound_block = Experiment.get_block(self.bound_variable_block)
        es = getattr(bound_block, self.bound_variable_field)

        if not isinstance(es, ExpressionSet):
            return ""
        return es.to_json_preview(200)


def get_block_class_by_name(name):
    if name in block_classes_by_name.keys():
        return block_classes_by_name[name]
    else:
        raise KeyError("No such plugin: %s" % name)

#TODO: move to DB
block_classes_by_name = {}
blocks_by_group = defaultdict(list)


def register_block(code_name, human_title, group, cls):
    block_classes_by_name[code_name] = cls
    blocks_by_group[group].append({
        "name": code_name,
        "title": human_title,
    })


class GroupType(object):
    INPUT_DATA = "Input data"
    META_PLUGIN = "Meta plugins"
    VISUALIZE = "Visualize"

register_block("fetch_ncbi_gse", "Fetch NCBI GSE", GroupType.INPUT_DATA, FetchGSE)
register_block("get_bi_gene_set", "Get MSigDB gene set", GroupType.INPUT_DATA, GetBroadInstituteGeneSet)

register_block("cross_validation", "Cross validation", GroupType.META_PLUGIN, CrossValidation)

register_block("Pca_visualize", "2D PCA Plot", GroupType.VISUALIZE, PCA_visualize)


old_blocks_by_group = [
    ("Input data", dict([
        ("fetch_ncbi_gse", "Fetch NCBI GSE"),
        ("get_bi_gene_set", "Get MSigDB gene set"),
    ])),
    ("Conversion", dict([
        ("pca_aggregation", "PCA aggregation"),
        ("mean_aggregation", "Mean aggregation"),
    ])),
    ("Classifiers", dict([
        ("t_test", "T-test"),
        ("svm", "SVM"),
        ("dtree", "Decision tree"),
    ])),
    ("Meta plugins", dict([
        ("cross_validation", "Cross validation"),
    ])),
    ("Visualisation", dict([
        ("ES_details", "Detail view of Expression Set"),
        ("Pca_visualize", "2D PCA Plot"),
        ("boxplot", "Boxplot"),
        ("render_table", "Raw table")
    ])),
]