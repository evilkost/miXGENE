from fysom import Fysom

from workflow.ports import BlockPort

from wrappers.gt import global_test_task

from generic import GenericBlock


class GlobalTest(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variables', 'src': 'created', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'pca_computed', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'variable_bound', 'dst': 'variable_bound'},

            {'name': 'run_gt', 'src': 'variable_bound', 'dst': 'gt_computing'},

            {'name': 'success', 'src': 'gt_computing', 'dst': 'gt_computed'},
            {'name': 'error', 'src': 'gt_computing', 'dst': 'variable_bound'},
            ]
    })

    widget = "widgets/pca_view.html"
    block_base_name = "GLOBAL_TEST"
    all_actions = [
        ("bind_variables", "Select variable", True),
        ("run_gt", "Run Global test", True),

        ("success", "", False),
        ("error", "", False)
    ]

    elements = [
        "gt_result.html"
    ]

    def __init__(self, *args, **kwargs):
        super(GlobalTest, self).__init__("Global test", *args, **kwargs)
        self.result = None
        self.celery_task = None

        self.ports = {
            "input": {
                "es": BlockPort(name="es", title="Choose expression set",
                                data_type="ExpressionSet", scopes=[self.scope]),
                "gs": BlockPort(name="gs", title="Choose gene set",
                                data_type="GeneSets", scopes=[self.scope])
            }
        }

    def serialize(self, exp, to="dict"):
        hash = super(GlobalTest, self).serialize(exp, to)
        hash["gt_result"] = self.gt_result_in_dict()

        return hash

    def gt_result_in_dict(self):
        return ""
        # df = self.pca_result.get_pca()
        # return df.to_json(orient="split")

    def run_gt(self, exp, request, *args, **kwargs):
        self.clean_errors()
        es = self.get_var_by_bound_key_str(exp, self.ports["input"]["es"].bound_key)
        gs = self.get_var_by_bound_key_str(exp, self.ports["input"]["gs"].bound_key)

        self.celery_task = global_test_task.s(
            exp, self, "gt_df_storage",
            es, gs,
            exp.get_data_file_path("%s_gt_result" % self.uuid, "csv.gz")
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, result):
        self.result = result
        exp.store_block(self)

    def error(self, exp):
        exp.store_block(self)