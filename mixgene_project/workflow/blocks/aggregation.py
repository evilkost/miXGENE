import pandas as pd

from webapp.tasks import wrapper_task
from workflow.blocks.fields import ActionsList, ActionRecord, InputBlockField, ParamField, InputType, FieldType, \
    OutputBlockField
from workflow.blocks.generic import GenericBlock, execute_block_actions_list
from wrappers.aggregation import aggregation_task

def do_gs_agg(
    exp, block,
    es, gs, method,
    base_filename
):
    """
        @type es: ExpressionSet
    """
    result_es = es.clone(base_filename)
    result_es.store_pheno_data_frame(es.get_pheno_data_frame())
    gene_sets = gs.get_gs()

    df = es.get_assay_data_frame()

    df_list = []
    df_index_set = set(df.index)

    for set_name, gene_ids in gene_sets.genes.items():
        fixed_gene_ids = [gi for gi in gene_ids if gi in df_index_set]
        if fixed_gene_ids:
            sub_df = df.loc[fixed_gene_ids]
            if method == "mean":
                row = sub_df.mean()
            if method == "median":
                row = sub_df.mean()

            df_list.append((set_name, row))

    result_df = pd.DataFrame(dict(df_list)).T
    result_es.store_assay_data_frame(result_df)

    return [result_es], {}


class GeneSetAgg(GenericBlock):
    block_base_name = "GENE_SET_AGG"
    is_block_supports_auto_execution = True

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
        ])
    _block_actions.extend(execute_block_actions_list)

    _es = InputBlockField(name="es", order_num=10,
                                required_data_type="ExpressionSet", required=True)
    _gs = InputBlockField(name="gs", order_num=20,
                                required_data_type="GeneSets", required=True)

    agg_method = ParamField(
        "agg_method", title="Aggregate method", order_num=50,
        input_type=InputType.SELECT, field_type=FieldType.STR,
        init_val="mean",
        options={
            "inline_select_provider": True,
            "select_options": [
                ["mean", "Mean"],
                ["media", "Median"]
            ]
        }
    )

    agg_es = OutputBlockField(name="agg_es", provided_data_type="ExpressionSet")

    def __init__(self, *args, **kwargs):
        super(GeneSetAgg, self).__init__("Gene sets level aggregation", *args, **kwargs)
        self.celery_task = None

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()
        es = self.get_input_var("es")
        gs = self.get_input_var("gs")

        base_filename = "%s_gs_agg" % (self.uuid, )

        self.celery_task = wrapper_task.s(
            do_gs_agg,
            exp, self,
            es, gs, self.agg_method,
            base_filename
        )

        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, agg_es):
        self.set_out_var("agg_es", agg_es)
        exp.store_block(self)


class SvdSubAgg(GenericBlock):
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

    c = ParamField(name="c", title="Constant c",
                   input_type=InputType.TEXT, field_type=FieldType.FLOAT, init_val=1.0)

    agg_es = OutputBlockField(name="agg_es", provided_data_type="ExpressionSet")

    mode = ""

    def __init__(self, *args, **kwargs):
        super(SvdSubAgg, self).__init__(*args, **kwargs)
        self.celery_task = None

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()
        mRNA_es = self.get_input_var("mRNA_es")
        miRNA_es = self.get_input_var("miRNA_es")
        interaction_matrix = self.get_input_var("interaction")

        self.celery_task = wrapper_task.s(
            aggregation_task,
            exp, self,
            mode=self.mode,
            c=self.c,
            m_rna_es=mRNA_es,
            mi_rna_es=miRNA_es,
            interaction_matrix=interaction_matrix,
            base_filename="%s_%s_agg" % (self.uuid, self.mode)
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, agg_es):
        self.set_out_var("agg_es", agg_es)
        exp.store_block(self)


class SubAggregation(SvdSubAgg):
    block_base_name = "SUB_AGG"
    mode = "SUB"

    def __init__(self, *args, **kwargs):
        super(SubAggregation, self).__init__("Sub aggregation", *args, **kwargs)


class SvdAggregation(SvdSubAgg):
    block_base_name = "SVD_AGG"
    mode = "SVD"

    def __init__(self, *args, **kwargs):
        super(SvdAggregation, self).__init__("Svd aggregation", *args, **kwargs)

