__author__ = 'kost'
from blocks import FetchGSE


plugins_by_name = {
    "fetch_ncbi_gse": FetchGSE,
}


def get_plugin_by_name(name):
    if name in plugins_by_name.keys():
        return plugins_by_name[name]
    else:
        raise KeyError("No such plugin: %s" % name)