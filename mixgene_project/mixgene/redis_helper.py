from mixgene.util import get_redis_instance


class ExpKeys(object):
    @staticmethod
    def get_exp_notify_publish_key(exp_id):
        return "ENPK-%s" % exp_id

    @staticmethod
    def get_blocks_uuid_by_alias(exp_id):
        # redis hash set
        return "GBUIA-%s" % exp_id

    @staticmethod
    def get_scope_creating_block_uuid_keys(exp_id):
        # redis hash set "scope" -> "block_uuid"
        return "SCBU-%s" % exp_id

    @staticmethod
    def get_exp_blocks_list_key(exp_id):
        # TODO: remove this, since we alseo have blocks_uuid_by_alias
        return "EBS-%s" % exp_id

    @staticmethod
    def get_block_key(block_uuid):
        return "BLOCK-%s" % block_uuid

    @staticmethod
    def get_all_exp_keys_key(exp_id):
        return "AERK-%s" % exp_id

    @staticmethod
    def get_scope_key(exp_id, scope_name):
        """
            Set of scope_name vars see workflow.scope.ScopeVar
        """
        return "S_ESCP-%s-%s" % (exp_id, scope_name)

    @staticmethod
    def get_auto_exec_task_lock_key(exp_id, scope_name):
        return "AETLK-%s-%s" % (exp_id, scope_name)

    @staticmethod
    def get_block_global_lock_key(exp_id, block_uuid):
        return "MBCLK-%s-%s" % (exp_id, block_uuid)


def register_sub_key(exp_id, key, redis_instance=None):
    if redis_instance is None:
        r = get_redis_instance()
    else:
        r = redis_instance
    r.sadd(ExpKeys.get_all_exp_keys_key(exp_id), key)
