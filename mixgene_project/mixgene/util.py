from collections import defaultdict
from redis import StrictRedis
from settings import REDIS_HOST, REDIS_PORT
from subprocess import Popen, PIPE
import os
from urlparse import urlparse
import re

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

def get_redis_instance():
    return StrictRedis(host=REDIS_HOST, port=REDIS_PORT)


def dyn_import(class_name):
    # with parent module
    components = class_name.split('.')
    mod = __import__('.'.join(components[:-1]))
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def mkdir(path):
    args = ['mkdir', '-p', path]
    output = Popen(args, stdout=PIPE, stderr=PIPE)
    retcode = output.wait()


def fetch_file_from_url(url, target_file):
    try:
        os.rm(target_file)
    except:
        pass
    args = ['wget', '-nv', '-t', '3', '-O', str(target_file), str(url)]
    log.debug('Command to execute: %s' , ' '.join(args))
    #print "args", args
    output = Popen(args, stdout=PIPE, stderr=PIPE)
    retcode = output.wait()
    pipe = output.communicate()
    log.debug("Return code: %s, stdout: %s", retcode, pipe)

    if os.path.getsize(target_file) == 0:
        raise RuntimeError("Got empty file, something bad happaned")


def clean_GEO_file(src_path, target_path):
    with open(src_path, 'r') as src, open(target_path, 'w') as target:
        for line in src:
            if len(line)==0 or line[0] == "!" or line in [" ", " \n", "\n", "\r\n"]:
                pass
            else:
                target.write(line)


def geo_folder_name(prefix, uid):
    """
        prefix: "GSE", "GPL" ...
        uid: numeric uid
    """
    suid = str(uid)
    if len(suid) <= 3:
        return prefix + "nnn"
    else:
        return prefix + suid[0:-3]  + 'nnn'


NCBI_GEO_ROOT = "ftp://ftp.ncbi.nlm.nih.gov/geo"
NCBI_GEO_SERIES = NCBI_GEO_ROOT + "/series"


def prepare_GEO_ftp_url(geo_uid, file_format):
    """
        Supported types:
            db: "GSE",
            format: "txt", "soft"
    """
    db_type = geo_uid[:3]
    uid = geo_uid[3:]

    if db_type == "GSE":
        pre_url = "%s/%s/%s" % (NCBI_GEO_SERIES, geo_folder_name(db_type, uid), db_type + str(uid))
        if file_format == "txt":
            filename = "%s_series_matrix" % geo_uid
            compressed_filename = filename + ".txtgz"
            url = "%s/matrix/%s" % (pre_url, compressed_filename)
        elif file_format == "soft":
            filename = "%s_family" % geo_uid
            compressed_filename = filename + ".soft.gz"
            url = "%s/soft/%s" % (pre_url, compressed_filename)
        else:
            raise Exception("format %s isn't supported yet" % file_format)
    else:
        raise Exception("db_type %s isn't supported yet" % db_type)

    return url, compressed_filename, filename


def transpose_dict_list(gene_sets):
    set_by_gene = defaultdict(list)
    for set_id, genes in gene_sets.iteritems():
        for gen in genes:
            set_by_gene[gen].append(set_id)
    return set_by_gene
