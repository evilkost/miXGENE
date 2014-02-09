from uuid import uuid1
#TODO: Maybe somehow merge with django form fields


class AbsInputVar(object):
    def __init__(self, name, title, description, *args, **kwargs):
        self.name = name
        self.title = title
        self.description = description
        self.uuid = str(uuid1())

        self.required = kwargs.get("required", False)
        self.error = None


class InputGroup(AbsInputVar):
    def __init__(self, *args, **kwargs):
        super(InputGroup, self).__init__(*args, **kwargs)
        self.input_type = "group"
        self.inputs = kwargs.get("inputs", {})


class CheckBoxInputVar(AbsInputVar):
    def __init__(self, *args, **kwargs):
        super(CheckBoxInputVar, self).__init__(*args, **kwargs)
        self.input_type = "checkbox"
        self.value = kwargs.get('is_checked', True)


class NumericInputVar(AbsInputVar):
    def __init__(self, *args, **kwargs):
        super(NumericInputVar, self).__init__(*args, **kwargs)
        self.input_type = "numeric_field"
        self.value = kwargs.get("default", 0.3)
        self.is_integer = False


