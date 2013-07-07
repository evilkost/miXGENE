from redis import Redis
from settings import REDIS_HOST, REDIS_PORT

def get_redis_instance():
    return Redis(host=REDIS_HOST, port=REDIS_PORT)


def dyn_import(class_name):
    # with parent module
    components = class_name.split('.')
    mod = __import__('.'.join(components[:-1]))
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod
