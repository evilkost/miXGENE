# -*- coding: utf-8 -*-

from redis import StrictRedis
from pymongo import MongoClient
from bson.objectid import ObjectId


class BlockStorage(object):

    def __init__(self, db_provider):
        self.db_provider = db_provider

    def store(self, block):
        self.db_provider.save(block.db_key, block.serialize())

    def load(self, klass, key):
        raw = self.db_provider.load(key)
        return klass.deserialize(raw)


class RedisProvider(object):

    def __init__(self, host=None, port=None, db=None):

        self.conn = StrictRedis(host, port, db)

    def get_conn(self):
        return self.conn


class MongoProvider(object):

    def __init__(self, host=None, port=None):
        self.conn = MongoClient(host, port)

