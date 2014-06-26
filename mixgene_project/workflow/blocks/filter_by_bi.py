# -*- coding: utf-8 -*-

from webapp.tasks import wrapper_task
from workflow.blocks.blocks_pallet import GroupType
from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InputBlockField, InputType, ParamField, \
    ActionRecord, ActionsList

from workflow.blocks.generic import GenericBlock, save_params_actions_list, execute_block_actions_list

from wrappers.aggregation import aggregation_task


def filter_by_bi(
    exp, block,
    m_rna_es, mi_rna_es, interaction_matrix,
    base_filename
):

    m_rna_df = m_rna_es.get_assay_data_frame()
    mi_rna_df = mi_rna_es.get_assay_data_frame()
    targets_matrix = interaction_matrix.load_matrix()

    allowed_m_rna_index_set = set(targets_matrix.columns) & set(m_rna_df.index)
    m_rna_df_filtered = m_rna_df.loc[allowed_m_rna_index_set, :]

    allowed_mi_rna_index_set = set(targets_matrix.index) & set(mi_rna_df.index)
    mi_rna_df_filtered = mi_rna_df.loc[allowed_mi_rna_index_set, :]

    #result_df = agg_func(m_rna, mi_rna, targets_matrix, c)
    m_rna_result = m_rna_es.clone(base_filename + "_mRNA")
    m_rna_result.store_assay_data_frame(m_rna_df_filtered)
    m_rna_result.store_pheno_data_frame(m_rna_es.get_pheno_data_frame())

    mi_rna_result = mi_rna_es.clone(base_filename + "_miRNA")
    mi_rna_result.store_assay_data_frame(mi_rna_df_filtered)
    mi_rna_result.store_pheno_data_frame(mi_rna_es.get_pheno_data_frame())

    return [m_rna_result, mi_rna_result], {}


class FilterByInteraction(GenericBlock):
    block_base_name = "FILTER_BY_BI"
    name = "Filter ES by interaction"
    block_group = GroupType.PROCESSING
    is_block_supports_auto_execution = True

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

    ])
    _block_actions.extend(execute_block_actions_list)

    _mRNA_es = InputBlockField(name="mRNA_es", order_num=10,
                               required_data_type="ExpressionSet", required=True)
    _miRNA_es = InputBlockField(name="miRNA_es", order_num=20,
                                required_data_type="ExpressionSet", required=True)
    _interaction = InputBlockField(name="interaction", order_num=30,
                                   required_data_type="BinaryInteraction", required=True)

    m_rna_filtered_es = OutputBlockField(name="m_rna_filtered_es", provided_data_type="ExpressionSet")
    mi_rna_filtered_es = OutputBlockField(name="mi_rna_filtered_es", provided_data_type="ExpressionSet")

    def __init__(self, *args, **kwargs):
        super(FilterByInteraction, self).__init__(*args, **kwargs)
        self.celery_task = None

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()
        mRNA_es = self.get_input_var("mRNA_es")
        miRNA_es = self.get_input_var("miRNA_es")
        interaction_matrix = self.get_input_var("interaction")

        self.celery_task = wrapper_task.s(
            filter_by_bi,
            exp, self,

            m_rna_es=mRNA_es,
            mi_rna_es=miRNA_es,
            interaction_matrix=interaction_matrix,
            base_filename="%s_filtered_by_BI" % self.uuid,
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, m_rna_filtered_es, mi_rna_filtered_es):
        self.set_out_var("m_rna_filtered_es", m_rna_filtered_es)
        self.set_out_var("mi_rna_filtered_es", mi_rna_filtered_es)
        exp.store_block(self)