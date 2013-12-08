class BlockPort(object):
    scopes = ["root"]

    def __init__(self, name, title, data_type, scopes=None):
        """
        @param name: Internal variable name, should be unique across block
        @param title: Human readable title
        @param data_type: Respective data type for variable
        @param scopes: List of scopes, that should be accessible by this port
        @return: BlockPort instance
        """
        self.name = name
        self.title = title
        self.data_type = data_type
        self.scopes = scopes or ['root']

        self.options = {}
        self.bound_key = None
        self.is_editable = True

    @property
    def bound(self):
        if self.bound_key is None or self.bound_key not in self.options.keys():
            return None
        else:
            return self.options[self.bound_key]

    def serialize(self):
        return {
            "name": self.name,
            "data_type": self.data_type,
            "scopes": self.scopes,

            "options": [
                {
                    "key": bound_var.key,
                    "obj": bound_var.serialize()
                }
                for bound_var in sorted(
                    self.options.values(),
                    key=lambda x: "%s%s" % (x.block_alias, x.var_name),)
            ],
            "bound_key": self.bound_key
            #"is_editable": self.is_editable
        }


class BoundVar(object):
    def __init__(self, block_uuid, block_alias, var_name):
        self.block_uuid = block_uuid
        self.block_alias = block_alias
        self.var_name = var_name

    @property
    def key(self):
        return "%s_%s" % (self.block_uuid, self.var_name)

    def serialize(self):
        return {
            "block_uuid": self.block_uuid,
            "block_alias": self.block_alias,
            "var_name": self.var_name,
        }