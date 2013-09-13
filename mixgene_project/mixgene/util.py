from redis import Redis
from settings import REDIS_HOST, REDIS_PORT
from subprocess import Popen, PIPE
import os
from urlparse import urlparse
import re

def get_redis_instance():
    return Redis(host=REDIS_HOST, port=REDIS_PORT)

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

def fetch_file_from_url(url, target_file, do_unpuck=True):
    try:
        os.rm(target_file)
    except:
        pass
    args = ['wget', '-nv', '-t', '3', '-O', target_file, url]
    #print "args", args
    output = Popen(args, stdout=PIPE, stderr=PIPE)
    retcode = output.wait()
    pipe=output.communicate()
    #print "retcode", retcode
    #print "pipe", pipe
    if retcode != 0:
        raise Exception("failed to fetch file %s" % url)
    else:
        if do_unpuck:
            args = ['gunzip', '-vf', target_file]
            #print "args", args
            output = Popen(args, stdout=PIPE, stderr=PIPE)
            retcode = output.wait()
            pipe = output.communicate()
            #print "gunzip pipe", pipe
            if retcode != 0:
                raise Exception("failed to unpack file %s " % target_file)


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

def prepare_GEO_ftp_url(uid, db_type, format_type):
    """
        Supported types:
            db: "GSE",
            format: "matrix",
    """
    if db_type == "GSE":
        pre_url = "%s/%s/%s" % (NCBI_GEO_SERIES, geo_folder_name(db_type, uid), db_type + str(uid))
        if format_type == "matrix":
            filename = "%s%s_series_matrix.txt" % (db_type, uid)
            compressed_filename = filename + ".gz"
            url = "%s/matrix/%s" % (pre_url, compressed_filename)
    return url, compressed_filename, filename




