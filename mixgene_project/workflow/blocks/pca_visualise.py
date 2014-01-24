from fysom import Fysom

from workflow.ports import BlockPort
from workflow.wrappers import pca_test

from generic import GenericBlock


class PCA_visualize(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variables', 'src': 'created', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'pca_computed', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'variable_bound', 'dst': 'variable_bound'},

            {'name': 'run_pca', 'src': 'variable_bound', 'dst': 'pca_computing'},

            {'name': 'success', 'src': 'pca_computing', 'dst': 'pca_computed'},
            {'name': 'error', 'src': 'pca_computing', 'dst': 'variable_bound'},
        ]
    })

    widget = "widgets/pca_view.html"
    block_base_name = "PCA_VIEW"
    all_actions = [
        ("bind_variables", "Select variable", True),
        ("run_pca", "Run PCA", True),

        ("success", "", False),
        ("error", "", False)
    ]

    elements = [
        "pca_result.html"
    ]

    def __init__(self, *args, **kwargs):
        super(PCA_visualize, self).__init__("PCA Analysis", *args, **kwargs)
        self.pca_result = None
        self.celery_task = None

        self.ports = {
            "input": {
                "es": BlockPort(name="es", title="Choose expression set",
                                data_type="ExpressionSet", scopes=[self.scope]),
                }
        }

    @property
    def pca_result_in_json(self):
        df = self.pca_result.get_pca()
        return df.to_json(orient="split")

    def run_pca(self, exp, request, *args, **kwargs):
        self.clean_errors()
        es = self.get_var_by_bound_key_str(exp, self.ports["input"]["es"].bound_key)
        self.celery_task = pca_test.s(exp, self, es)
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp):
        exp.store_block(self)

    def error(self, exp):
        exp.store_block(self)