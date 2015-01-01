# -*- coding: utf-8 -*-

import time
import pytest

import pymongo
from bson.objectid import ObjectId



from marshmallow import Schema, fields, pprint

from workflow.block_ng import get_schema_class, ClassifierBlock, FetchGseNg


def _test_get_schema():
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

        self.exp_id = 1234
        self.owner_id = 1

    def teardown_method(self, method):
        self.test_client.drop_database(self.db_name)

    def not_test_db_connection(self):
        self.db["collection_1"].insert({"foo": "bar"})
        pprint(list(self.db["collection_1"].find()))

    def test_basic_block(self):

        block = ClassifierBlock.new(self.exp_id, 1)

        #print()
        pprint(block._doc, indent=4)

        print(block.uuid)
        # block.save(self.db)
        # uuid = block.uuid
        #
        # retrieved_block = ClassifierBlock.load(self.db, self.exp_id, uuid)
        # pprint(retrieved_block._doc)

    def test_meta_magic_1(self):

        block = FetchGseNg.new(self.exp_id, self.owner_id)
        # pprint(block.__class__.__dict__, indent=2)
        pprint(block._doc)