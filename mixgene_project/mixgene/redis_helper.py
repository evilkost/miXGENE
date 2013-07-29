class ExpKeys(object):
    @staticmethod
    def get_context_store_key(exp_id):
        return "CSTP-%s" % exp_id

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
