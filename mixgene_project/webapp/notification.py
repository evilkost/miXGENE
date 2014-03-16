# -*- coding: utf-8 -*-
import json
import logging
from mixgene.util import get_redis_instance
from mixgene.redis_helper import ExpKeys

log = logging.getLogger("mixgene.notify")
log.setLevel(logging.DEBUG)


class NotifyType(object):
    ALL = "updated_all"
    BLOCK = "updated_block"
    SCOPE = "updated_scope"


class NotifyMode(object):
    INFO = "info"
    WARN = "warning"
    ERROR = "error"
    SUCCESS = "success"


class Notification(object):
    def __init__(self, exp_id, type_,
                 comment=None, silent=True, mode=None):
        self.exp_id = exp_id
        self.type_ = type_
        self.mode = mode or NotifyMode.INFO
        self.silent = silent
        self.comment = comment

    def send(self):
        msg = self.to_dict()
        r = get_redis_instance()
        r.publish(ExpKeys.get_exp_notify_publish_key(self.exp_id),
                  json.dumps(msg))

        log.debug("Sent notification: %s", json.dumps(msg))

    def to_dict(self):
        return {
            "exp_id": self.exp_id,
            "type": self.type_,
            "silent": self.silent,
            "comment": self.comment,
            "mode": self.mode
        }


class BlockUpdated(Notification):
    def __init__(self, exp_id, block_uuid, block_alias, **kwargs):
        super(BlockUpdated, self).__init__(exp_id, type_=NotifyType.BLOCK, **kwargs)
        self.block_uuid = block_uuid
        self.block_alias = block_alias

    def to_dict(self):
        res = super(BlockUpdated, self).to_dict()
        res["block_uuid"] = self.block_uuid
        res["block_alias"] = self.block_alias
        if self.comment is None:
            self.comment = "Block %s was updated" % self.block_alias
        return res


class AllUpdated(Notification):
    def __init__(self, exp_id, **kwargs):
        super(AllUpdated, self).__init__(exp_id, type_=NotifyType.ALL, **kwargs)
