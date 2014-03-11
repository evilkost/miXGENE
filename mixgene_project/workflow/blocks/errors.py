# -*- coding: utf-8 -*-


# TODO: sadly we cann't use read Exception as a base class
class ConfigurationError(object):
    def __init__(self, msg, *args, **kwargs):
        self.msg = msg


class PortError(ConfigurationError):
    def __init__(self, block, port_name, *args, **kwargs):
        super(PortError, self).__init__(*args, **kwargs)
        self.block_uuid = block.uuid
        self.block_alias = block.base_name
        self.port_name = port_name

    def __str__(self):
        return "%(msg)s : port `%(port_name)s` in block `%(block_alias)s`" % self.__dict__
            # [uuid: %(block_uuid)s]" %\
