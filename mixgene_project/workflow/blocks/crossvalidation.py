from copy import deepcopy
import logging
from pprint import pprint
from mixgene.redis_helper import ExpKeys

from webapp.models import Experiment
from environment.structures import SequenceContainer
from webapp.scope import ScopeRunner, ScopeVar
from webapp.tasks import wrapper_task
from workflow.blocks.meta_block import UniformMetaBlock
from workflow.common_tasks import generate_cv_folds
from generic import InnerOutputField
from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, OutputBlockField, InputBlockField, IteratedInnerFieldManager
from mixgene.util import get_redis_instance
import redis_lock

# class CrossValidationForm(forms.Form):
#     folds_num = forms.IntegerField(min_value=2, max_value=100)
#     #split_ratio = forms.FloatField(min_value=0, max_value=1)
#

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class CrossValidation(UniformMetaBlock):
    block_base_name = "CROSS_VALID"

    _cv_actions = ActionsList([
        ActionRecord("become_ready", ["valid_params"], "ready")
    ])
    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "cv_info.html"
    ])

    _input_es_dyn = InputBlockField(
        name="es_inputs", required_data_type="ExpressionSet",
        required=True, multiply_extensible=True
    )

    folds_num = ParamField(name="folds_num", title="Folds number",
                           input_type=InputType.TEXT, field_type=FieldType.INT, init_val=10)

    def __init__(self, *args, **kwargs):
        super(CrossValidation, self).__init__("Cross Validation", *args, **kwargs)

    def get_fold_labels(self):
        return ["fold_%s" % (num + 1, ) for num in range(self.folds_num)]

    def add_dyn_input_hook(self, exp, dyn_port, new_port):
        """
            @type new_port: InputBlockField
        """
        new_inner_output_train = InnerOutputField(
            name="%s_train_i" % new_port.name,
            provided_data_type=new_port.required_data_type
        )
        new_inner_output_test = InnerOutputField(
            name="%s_test_i" % new_port.name,
            provided_data_type=new_port.required_data_type
        )
        self.inner_output_es_names_map[new_port.name] = \
            (new_inner_output_train.name, new_inner_output_test.name)

        self.register_inner_output_variables([new_inner_output_train, new_inner_output_test])

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()

        self.inner_output_manager.reset()
        es_dict = {
            inp_name: self.get_input_var(inp_name)
            for inp_name in self.es_inputs
        }

        self.celery_task = wrapper_task.s(
            generate_cv_folds,
            exp, self,
            folds_num=self.folds_num,
            es_dict=es_dict,
            inner_output_es_names_map=self.inner_output_es_names_map,
            success_action="on_folds_generation_success",
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def on_params_is_valid(self, exp, *args, **kwargs):
        super(CrossValidation, self).on_params_is_valid(exp, *args, **kwargs)
        self.do_action("become_ready", exp)

    def become_ready(self, *args, **kwargs):
        pass