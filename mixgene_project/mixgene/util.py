from redis import Redis
from settings import REDIS_HOST, REDIS_PORT

def get_redis_instance():
    return Redis(host=REDIS_HOST, port=REDIS_PORT)

