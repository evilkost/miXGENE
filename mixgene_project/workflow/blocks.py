from uuid import uuid1

from django import forms

from structures import ExpressionSet, PlatformAnnotation


class GenericBlock(object):
    def __init__(self, name, type):
        """
            Building block for workflow
            @type can be: "user_input", "computation"
        """
        self.uuid = uuid1().hex
        self.name = name
        self.type = type

        # pairs of (var name, data type, default name in context)
        self.required_inputs = []
        self.provide_outputs = []

        self.widget = None

    #def is_applicable(self, ctx):
    #    return True

    def is_runnable(self, ctx):
        return False

    def is_configurable(self, ctx):
        return True

    def is_configurated(self, ctx):
        return False

    def is_visible(self, ctx):
        return True


class FetchGseForm(forms.Form):
    # Add custom validator to check GSE prefix
    geo_uid = forms.CharField(min_length=4, max_length=31, required=True)
    expression_set_name = forms.CharField(label="Name for expression set",
                                          max_length=255)
    gpl_annotation_name = forms.CharField(label="Name for GPL annotation",
                                          max_length=255)

class FetchGSE(GenericBlock):
    def __init__(self):
        super(FetchGSE, self).__init__("Fetch ncbi gse", "user_input")

        self.provide_outputs = [
            ("expression_set", ExpressionSet, "expression"),
            ("gpl_annotation", PlatformAnnotation, "annotation"),
        ]

        self.widget = "widgets/fetch_ncbi_gse.html"

        self.form_data = {
            "expression_set_name": "expression",
            "gpl_annotation_name": "annotation",
        }

        self.form_cls = FetchGseForm
        self.form = self.form_cls(self.form_data)

