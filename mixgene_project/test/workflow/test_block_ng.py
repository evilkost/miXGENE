# -*- coding: utf-8 -*-

import time
import pytest

import pymongo
from bson.objectid import ObjectId



from marshmallow import Schema, fields, pprint

from workflow.block_ng import get_schema_class, ClassifierBlock


def test_get_schema():
    spec = [
        ("name", fields.Str, [], {}),
        ("uuid", fields.Int, [], {"required": True, "default": None}),
    ]
    schema = get_schema_class(spec)(strict=True)


    raw_data = {
        "name": "generic block",
        "uuid": "21",
    }
    # print()
    # print("="*30)
    # pprint(schema.__dict__)
    result = schema.load(raw_data)
    # pprint(result)
    # print("="*30)


class TestMongoBlocks(object):

    def setup_method(self, method):
        self.test_client = pymongo.MongoClient()
        self.db_name = "test_{}".format(int(time.time()))
        self.db = self.test_client[self.db_name]

    def teardown_method(self, method):
        self.test_client.drop_database(self.db_name)

    def test_db_connection(self):
        self.db["collection_1"].insert({"foo": "bar"})
        pprint(list(self.db["collection_1"].find()))

    def test_basic_block(self):
        exp_id = 1234
        block = ClassifierBlock.new(exp_id, 1)

        print()
        pprint(block._doc, indent=4)
        block.save(self.db)
        uuid = block.uuid

        retrieved_block = ClassifierBlock.load(self.db, exp_id, uuid)
        pprint(retrieved_block._doc)


