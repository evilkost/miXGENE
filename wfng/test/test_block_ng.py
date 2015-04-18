# -*- coding: utf-8 -*-

import time
import pytest

from pymongo import MongoClient
from bson.objectid import ObjectId

from marshmallow import Schema, fields, pprint
import sys
sys.path.append("..")
sys.path.append("../..")

from mgn2.store.db import MongoProvider
from mgn2.wf.block import GenericBlock

# def _test_get_schema():
#     spec = [
#         ("name", fields.Str, [], {}),
#         ("uuid", fields.Int, [], {"required": True, "default": None}),
#     ]
#     schema = get_schema_class(spec)(strict=True)
#
#     raw_data = {
#         "name": "generic block",
#         "uuid": "21",
#     }
#     # print()
#     # print("="*30)
#     # pprint(schema.__dict__)
#     result = schema.load(raw_data)


class TestMongoBlocks(object):

    def setup_method(self, method):
        self.db = MongoProvider()
        # self.test_client = pymongo.MongoClient()
        # # self.db_name = "test_{}".format(int(time.time()))
        # self.db_name = "test_123"
        # self.db = self.test_client[self.db_name]
        #
        # self.db_provider = DbProvider(self.db)
        #
        # self.exp_id = 1234 + int(time.time())
        # self.owner_id = 1

    # @pytest.fixture
    # def root_scope(self):
    #     self.root_scope = Scope(exp_id=self.exp_id, name="root")
    #     self.root_scope.next_et()
    #     self.root_scope.save(self.db)

    def teardown_method(self, method):
        pass
        # self.test_client.drop_database(self.db_name)

    def test_create_generic_block(self):
        gb = GenericBlock.create()


    # def not_test_db_connection(self):
    #     self.db["collection_1"].insert({"foo": "bar"})
    #     pprint(list(self.db["collection_1"].find()))

    # def test_basic_block(self, root_scope):
    #
    #     block = ClassifierBlock.new(self.exp_id, 1, self.root_scope)
    #
    #     #print()
    #     pprint(block._doc, indent=4)
    #
    #     print(block.uuid)
    #     # block.save(self.db)
    #     # uuid = block.uuid
    #     #
    #     # retrieved_block = ClassifierBlock.load(self.db, self.exp_id, uuid)
    #     # pprint(retrieved_block._doc)

    # def test_fetch_gse(self, root_scope):
    #
    #     block = FetchGseNg.new(self.exp_id, self.owner_id, self.root_scope)
    #     # pprint(block.__class__.__dict__, indent=2)
    #     # pprint(block._doc)
    #     pprint(block.uuid)
    #     # pprint(dict(block.__class__.__dict__), indent=2)
    #     block.do_user_action(self.db, "save_changes")
    #     uuid = block.uuid
    #     block2 = FetchGseNg.load(self.db, self.exp_id, uuid)
    #     pprint(block2._doc["errors"])
    #
    #     block2.register_db_provider(self.db_provider)
    #     block2.start_preprocess()
    #     for i in range(10):
    #         time.sleep(1)
    #         block_new = FetchGseNg.load(self.db, self.exp_id, uuid)
    #         print(">>>>> {}".format(block_new._user_fsm_state))



    # def test_cv_block(self, root_scope):
    #     block = CrossValidation.new(self.exp_id, self.owner_id, self.root_scope)
    #     pprint(block._doc)
    #
    # def test_scope(self):
    #     scope_orig = Scope(exp_id=self.exp_id, name="root")
    #     print(scope_orig)
    #     scope_orig.next_et()
    #     scope_orig.save(self.db)
    #     print(scope_orig)
    #     scope_from_bd = Scope.load(self.db, self.exp_id, "root")
    #     print(scope_from_bd)