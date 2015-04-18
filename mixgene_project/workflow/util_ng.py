# -*- coding: utf-8 -*-


def load_by_class(conn_db, klass, exp_id, block_uuid):
    coll = conn_db[klass._COLLECTION_NAME]
    raw = coll.find_one({
        "exp_id": exp_id,
        "uuid": block_uuid,
    })
    # from celery.contrib import rdb; rdb.set_trace()
    block = klass(exp_id, raw["owner_id"], raw["uuid"])
    block._deserialize_from_mongo(raw)
    print("load done")
    return block