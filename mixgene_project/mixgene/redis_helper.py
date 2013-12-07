from mixgene.util import get_redis_instance


class ExpKeys(object):
    @staticmethod
    def get_context_store_key(exp_id):
        return "CSTP-%s" % exp_id

    @staticmethod
    def get_blocks_uuid_by_alias(exp_id):
        # redis hash set
        return "GBUIA-%s" % exp_id

    @staticmethod
    def get_scope_vars_keys(exp_id):
        # redis hash set  "block_uuid:var_name"->
        #                   pickle( (scope, block_uuid, var_name, var_data_type))
        return "SV-%s" % exp_id

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
    def get_par_return_subtask_key(exp_id, parent_task_name):
        return "PRPR-%s-%s" % (exp_id, parent_task_name)

    @staticmethod
    def get_par_done_key(exp_id):
        return "PRPD-%s" % exp_id

    @staticmethod
    def get_par_context_result_key(exp_id, subtask_name):
        return "PRPC-%s-%s" % (exp_id, subtask_name)

    @staticmethod
    def get_all_exp_keys_key(exp_id):
        return "AERK-%s" % exp_id

    @classmethod
    def get_context_version_key(cls, exp_id):
        return "CVP-%s" % exp_id


def register_sub_key(exp_id, key, redis_instance=None):
    if redis_instance is None:
        r = get_redis_instance()
    else:
        r = redis_instance
    r.sadd(ExpKeys.get_all_exp_keys_key(exp_id), key)