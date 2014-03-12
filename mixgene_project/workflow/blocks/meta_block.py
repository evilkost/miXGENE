# -*- coding: utf-8 -*-
from abc import abstractmethod

from copy import deepcopy
import logging

import redis_lock

from mixgene.redis_helper import ExpKeys
from mixgene.util import get_redis_instance
from webapp.models import Experiment

from environment.structures import SequenceContainer
from environment.result_container import ResultsContainer

from webapp.scope import ScopeRunner, ScopeVar

from generic import InnerOutputField

from workflow.blocks.errors import PortError
from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, OutputBlockField, InputBlockField, IteratedInnerFieldManager


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class CollectorSpecification(object):
    def __init__(self):
        self.bound = {}  # name -> scope_vars
        self.label = ""

    def register(self, name, scope_var):
        """
            @type scope_var: ScopeVar
        """
        self.bound[name] = scope_var

    def remove(self, name):
        self.bound.pop(name)

    def to_dict(self, *args, **kwargs):
        return {
            "bound": {str(name): scope_var.to_dict()
                      for name, scope_var in self.bound.iteritems()},
            "new": {"name": "", "scope_var": ""},
            "label": self.label
        }


class UniformMetaBlock(GenericBlock):
    create_new_scope = True
    is_block_supports_auto_execution = True

    _block_actions = ActionsList([])
    _block_actions.extend(ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("add_collector_var", ["created", "ready", "done", "valid_params"], "validating_params"),
        ActionRecord("remove_collector_var", ["created", "ready", "done", "valid_params"], "validating_params"),

        ActionRecord("execute", ["ready"], "generating_folds", user_title="Run block"),

        ActionRecord("on_folds_generation_success", ["generating_folds"], "ready_to_run_sub_scope", reload_block_in_client=True),
        ActionRecord("continue_collecting_sub_scope", ["ready_to_run_sub_scope"],
                     "sub_scope_executing"),

        ActionRecord("run_sub_scope", ["ready_to_run_sub_scope"], "sub_scope_executing"),
        ActionRecord("on_sub_scope_done", ["sub_scope_executing"], "ready_to_run_sub_scope"),

        ActionRecord("success", ["working", "ready_to_run_sub_scope"], "done",
                     propagate_auto_execution=True, reload_block_in_client=True),
        ActionRecord("error", ["ready", "working", "sub_scope_executing",
                               "generating_folds", "ready_to_run_sub_scope"],
                     "execution_error", reload_block_in_client=True),

        ActionRecord("reset_execution", ["*", "done", "sub_scope_executing", "ready_to_run_sub_scope",
                                         "generating_folds", "execution_error"], "ready",
                     user_title="Reset execution"),
    ]))

    _collector_spec = ParamField(name="collector_spec", title="",
                                 field_type=FieldType.CUSTOM,
                                 input_type=InputType.HIDDEN,
                                 init_val=None
    )

    res_seq = BlockField(name="res_seq", provided_data_type="SequenceContainer",
                         field_type=FieldType.HIDDEN, init_val=None)

    _results_container = OutputBlockField(
        name="results_container",
        provided_data_type="ResultsContainer",
        field_type=FieldType.HIDDEN,
        init_val=None
    )

    def __init__(self, *args, **kwargs):
        super(UniformMetaBlock, self).__init__(*args, **kwargs)
        self.auto_exec_status_working.update(["sub_scope_executing", "ready_to_run_sub_scope",
                                              "generating_folds"])

        self.inner_output_manager = IteratedInnerFieldManager()
        self.collector_spec = CollectorSpecification()
        self.collector_spec.label = self.block_base_name + "_collection"

        self.inner_output_es_names_map = {}
        self.celery_task = None

        self.set_out_var("results_container", None)
        self.res_seq = SequenceContainer()

    @property
    def is_sub_pages_visible(self):
        if self.state in ['valid_params', 'done', 'ready']:
            return True
        return False

    @abstractmethod
    def get_fold_labels(self):
        pass

    def get_inner_out_var(self, name):
        return self.inner_output_manager.get_var(name)

    def run_sub_scope(self, exp, *args, **kwargs):
        self.reset_execution_for_sub_blocks()

        exp.store_block(self)
        sr = ScopeRunner(exp, self.sub_scope_name)
        sr.execute()

    def on_sub_scope_done(self, exp, *args, **kwargs):
        """
            @type exp: Experiment

            This action should be called by ScopeRunner
            when all blocks in sub-scope have exec status == done
        """
        r = get_redis_instance()
        with redis_lock.Lock(r, ExpKeys.get_metablock_collect_lock_key(self.exp_id, self.uuid)):

            cell = self.res_seq.sequence[self.inner_output_manager.iterator]
            for name, scope_var in self.collector_spec.bound.iteritems():
                var = exp.get_scope_var_value(scope_var)
                log.debug("Collected %s from %s", var, scope_var.title)
                if var is not None:
                    cell[name] = deepcopy(var)

            self.res_seq.sequence[self.inner_output_manager.iterator] = cell

            exp.store_block(self)

        if len(cell) < len(self.res_seq.fields):
            self.do_action("continue_collecting_sub_scope", exp)
        else:
            try:
                self.inner_output_manager.next()
                self.do_action("run_sub_scope", exp)
            except StopIteration, e:
                # All folds was processed without errors
                self.build_result_collection(exp)

                self.do_action("success", exp)

    def build_result_collection(self, exp):
        rc = ResultsContainer(
            base_dir=exp.get_data_folder(),
            base_filename="%s" % self.uuid
        )
        res_seq = self.res_seq

        axis_meta_block = self.base_name
        axis_meta_block_labels = self.get_fold_labels()

        # WARNING: We only support homogeneous results, so we only check first element

        def create_new_dim_rc(local_rc):
            local_rc.axis_list = [axis_meta_block]
            local_rc.labels_dict[axis_meta_block] = axis_meta_block_labels

            local_rc.init_ar()
            local_rc.update_label_index()

        # 4 cases: we have single input or not X we have ClassifierResults[s] or ResultsCollector[s]
        try:
            res_seq_field_name, data_type = res_seq.fields.iteritems().next()
        except Exception, e:
            log.exception(e)
            from celery.contrib import rdb
            rdb.set_trace()

        if data_type == "ClassifierResult":
            # if len(res_seq.fields) > 1:
            # we need to add 2 dimensions: meta block fold labels, and output collection names
            single_rc_list = []
            for field_name in res_seq.fields:
                rc_single = ResultsContainer("", "")
                create_new_dim_rc(rc_single)

                for idx, res_seq_cell in enumerate(res_seq.sequence):
                    rc_single.ar[idx] = res_seq_cell[field_name]
                single_rc_list.append(rc_single)

            rc.add_dim_layer(single_rc_list, self.collector_spec.label, res_seq.fields.keys())

            # else:
            #     create_new_dim_rc(rc)
            #     for idx, res_seq_cell in enumerate(res_seq.sequence):
            #         rc.ar[idx] = res_seq_cell[res_seq_field_name]

        elif data_type == "ResultsContainer":
            if len(res_seq.fields) > 1:
                raise Exception("Meta block only support single output of type ResultsContainer")
                # we need to add 2 dimensions: meta block fold labels, and output collection names
                # rc_list = []
                # for cell in res_seq.sequence:
                #     sub_rc_list = []
                #     rc_single = ResultsContainer("", "")
                #
                #     for field_name in res_seq.fields:
                #         sub_rc = cell[field_name]
                #         sub_rc.load()
                #         sub_rc_list.append(sub_rc)
                #     rc_single.add_dim_layer(sub_rc_list, self.collector_spec.label, res_seq.fields.keys())
                #     rc_list.append(rc_single)
                #
                # rc.add_dim_layer(rc_list, self.base_name, self.get_fold_labels())

            else:
                rc_list = []
                for cell in res_seq.sequence:
                    sub_rc = cell[res_seq_field_name]
                    sub_rc.load()
                    rc_list.append(sub_rc)

                rc.add_dim_layer(rc_list, self.base_name, self.get_fold_labels())

        elif data_type == "SequenceContainer":
            # TODO remove this check
            pass
        else:
            raise Exception("Meta blocks only support ClassifierResult "
                            "or ResultsContainer in the output collection. "
                            " Instead got: %s" % data_type)

        rc.store()
        rc.ar = None
        self.set_out_var("results_container", rc)

        # self.do_action("success", exp)

    def continue_collecting_sub_scope(self, exp, *args, **kwargs):
        pass

    def on_folds_generation_success(self, exp, sequence, *args, **kwargs):
        self.inner_output_manager.sequence = sequence
        self.inner_output_manager.next()

        self.res_seq.clean_content()
        self.res_seq.sequence = [{"__label__": label} for label in self.get_fold_labels()]

        exp.store_block(self)
        self.do_action("run_sub_scope", exp)

    def success(self, exp, *args, **kwargs):
        pass

    def update_res_seq_fields(self):
        self.res_seq.fields = {
            name: var.data_type
            for name, var in self.collector_spec.bound.iteritems()
        }

    def update_collector_label(self, exp, received_block, *args, **kwargs):
        label = received_block.get("collector_spec", {}).get("label")
        if label:
            self.collector_spec.label = label
            exp.store_block(self)

    def add_collector_var(self, exp, received_block, *args, **kwargs):
        rec_new = received_block.get("collector_spec", {}).get("new", {})
        if rec_new:
            name = str(rec_new.get("name"))
            scope_var_key = rec_new.get("scope_var")
            data_type = rec_new.get("data_type")
            if name and scope_var_key:
                scope_var = ScopeVar.from_key(scope_var_key)
                scope_var.data_type = data_type
                self.collector_spec.register(name, scope_var)

        self.update_res_seq_fields()
        exp.store_block(self)
        self.validate_params(exp, received_block, *args, **kwargs)

    def remove_collector_var(self, exp, received_block, *args, **kwargs):
        to_remove = received_block.get("collector_spec", {}).get("to_remove")
        if to_remove:
            log.debug("Trying to remove: %s", to_remove)
            self.collector_spec.remove(to_remove)

            self.update_res_seq_fields()
            exp.store_block(self)

        self.validate_params(exp, received_block, *args, **kwargs)

    def register_inner_output_variables(self, inner_outputs_list):
        scope = self.get_sub_scope()
        scope.load()

        for new_inner_output in inner_outputs_list:
            self.inner_output_manager.register(new_inner_output)
            self._block_serializer.register(new_inner_output)
            scope.register_variable(ScopeVar(
                self.uuid, new_inner_output.name, new_inner_output.provided_data_type))

        scope.store()

    def validate_params_hook(self, exp, *args, **kwargs):
        is_valid = True
        if self.collector_spec.bound:
            data_type_list = [scope_var.data_type for scope_var in self.collector_spec.bound.values()]
            data_types = list(set(data_type_list))
            if len(data_types) > 1:
                self.errors.append(
                    PortError(msg="Heterogeneous variables bound to the output collection",
                              block=self, port_name="(results collector)", block_alias=self.base_name)
                )
                is_valid = False
            elif data_types[0]  not in ["ClassifierResult", "ResultsContainer"]:
                self.errors.append(
                    PortError(msg="Data type `%s` is not allowed in the output collection" % data_types[0],
                              block=self, port_name="(results collector)", block_alias=self.base_name)
                )
                is_valid = False

        else:
            self.errors.append(
                PortError(msg="Block require at least one variable bound to the output collection",
                          block=self, port_name="(results collector)", block_alias=self.base_name)
            )
            is_valid = False

        return is_valid
