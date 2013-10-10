from uuid import uuid1


class AbsInputVar(object):
    def __init__(self, name, title, description, *args, **kwargs):
        self.name = name
        self.title = title
        self.description = description
        self.uuid = str(uuid1())


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


class FileInputVar(AbsInputVar):
    def __init__(self, *args, **kwargs):
        super(FileInputVar, self).__init__(*args, **kwargs)
        self.input_type = "file"
        self.is_done = False
        self.is_being_fetched = False

        self.file_type = None
        self.filename = None
        self.filepath = None
        self.file_format = None
        self.geo_uid = None

    def set_file_type(self, file_type):
        if file_type in ['user', 'ncbi_geo', 'gmt']:
            self.file_type = file_type
        else:
            raise Exception("file type should be in [`user`, `ncbi_geo`, `gmt`], not %s" % type)

