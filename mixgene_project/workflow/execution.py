from collections import defaultdict


class ExecStatus(object):
    WORKING = "working"
    READY = "ready"
    USER_REQUIRED = "user_required"
    DONE = "done"


class ScopeState(object):
    HALT = "halt"
    RUN = "run"


class ExecSupervisor(object):
    def __init__(self, exp, scope):
        """
            @type exp: webapp.models.Experiment

        """
        self.exp = exp
        self.scope = scope

    def build_dag(self):
        dag = defaultdict(list)
        #blocks_in_scope

    def run(self):
        ctx = self.exp.get_ctx()
