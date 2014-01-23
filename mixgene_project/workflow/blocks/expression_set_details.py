from fysom import Fysom

from workflow.ports import BlockPort
from environment.structures import ExpressionSet
from webapp.models import Experiment

from generic import GenericBlock


class ExpressionSetDetails(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variables', 'src': 'created', 'dst': 'variables_bound'},
            {'name': 'bind_variables', 'src': 'variables_bound', 'dst': 'variables_bound'},
            ]
    })

    widget = "widgets/expression_set_view.html"
    block_base_name = "ES_VIEW"
    all_actions = [
        ("bind_variables", "Select expression set", True)
    ]

    def __init__(self, *args, **kwargs):
        super(ExpressionSetDetails, self).__init__("Expression set details", "Visualisation", *args, **kwargs)
        self.bound_variable_field = None
        self.bound_variable_block = None
        self.bound_variable_block_alias = None

        self.variable_options = []

        self.ports = {
            "input": {
                "es": BlockPort(name="es", title="Choose expression set",
                                data_type="ExpressionSet", scopes=[self.scope])
            }
        }

    def bind_variables(self, exp, request, *args, **kwargs):
        self.clean_errors()
        split = request.POST['variable_name'].split(":")
        self.bound_variable_block = split[0]
        bound_block = exp.get_block(self.bound_variable_block)
        self.bound_variable_block_alias = bound_block.base_name
        self.bound_variable_field = ''.join(split[1:])
        exp.store_block(self)

    # def before_render(self, exp, *args, **kwargs):
    #     context_add = super(ExpressionSetDetails, self).before_render(exp, *args, **kwargs)
    #
    #     #import ipdb; ipdb.set_trace()
    #     available = exp.get_visible_variables(scopes=[self.scope], data_types=["ExpressionSet"])
    #     self.variable_options = prepare_bound_variable_select_input(
    #         available, exp.get_block_aliases_map(),
    #         self.bound_variable_block_alias, self.bound_variable_field)
    #
    #     if len(self.variable_options) == 0:
    #         self.errors.append(Exception("There is no blocks which provides Expression Set"))
    #         #return {"variable_options": available["ExpressionSet"]}
    #
    #     if self.state == "variables_bound":
    #         bound_block = Experiment.get_block(self.bound_variable_block)
    #         #import ipdb; ipdb.set_trace()
    #         if not isinstance(getattr(bound_block, self.bound_variable_field), ExpressionSet):
    #             self.errors.append(Exception("Bound variable isn't ready"))
    #
    #     self.errors.append({
    #         "msg": "Not implemented !"
    #     })
    #     return context_add

    def get_es_preview(self):
        if self.state != "variables_bound":
            return ""
        bound_block = Experiment.get_block(self.bound_variable_block)
        es = getattr(bound_block, self.bound_variable_field)

        if not isinstance(es, ExpressionSet):
            return ""
        return es.to_json_preview(200)