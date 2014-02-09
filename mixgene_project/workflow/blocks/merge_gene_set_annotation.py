from fysom import Fysom

from workflow.ports import BlockPort
from converters.gene_set_tools import merge_gs_with_platform_annotation

from generic import GenericBlock


class MergeGeneSetWithPlatformAnnotation(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variables', 'src': 'created', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'merge_done', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'variable_bound', 'dst': 'variable_bound'},

            {'name': 'run_merge', 'src': 'variable_bound', 'dst': 'in_merge'},

            {'name': 'success', 'src': 'in_merge', 'dst': 'done'},
            {'name': 'error', 'src': 'in_merge', 'dst': 'done'},

            ]
    })
    widget = "widgets/pca_view.html"
    block_base_name = "MERGE_GS_GPL_ANN"
    all_actions = [
        ("bind_variables", "Select variables", True),
        ("run_merge", "Run merge", True),

        ("success", "", False),
        ("error", "", False)
    ]
    provided_objects = {
        "gs_merged": "GmtStorage",
    }

    def __init__(self, *args, **kwargs):
        super(MergeGeneSetWithPlatformAnnotation, self).__init__(
            "Merge GeneSet with platform annotation", *args, **kwargs)

        self.celery_task = None
        self.gs_merged = None

        self.ports = {
            "input": {
                "ann": BlockPort(name="ann", title="Choose annotation",
                                 data_type="PlatformAnnotation", scopes=[self.scope]),
                "gs": BlockPort(name="gs", title="Choose gene set",
                                data_type="GmtStorage", scopes=[self.scope])
            }
        }

    def run_merge(self, exp, request, *args, **kwargs):
        """
        @type exp: webapp.models.Experiment
        """
        self.clean_errors()

        src_gs = self.get_var_by_bound_key_str(exp, self.ports["input"]["gs"].bound_key)
        ann = self.get_var_by_bound_key_str(exp, self.ports["input"]["ann"].bound_key)

        self.gs_merged = None
        self.celery_task = merge_gs_with_platform_annotation.s(
            exp, self, "gs_merged",
            src_gs, ann,
            exp.get_data_folder(), "%s_merged" % self.uuid
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, gs):
        self.gs_merged = gs
        exp.store_block(self)

    def error(self, exp):
        exp.store_block(self)
