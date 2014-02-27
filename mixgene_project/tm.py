
from webapp.models import Experiment
from webapp.scope import *


exp = Experiment.get_exp_by_id(56)
deps = exp.build_block_dependencies_by_scope("root")
dag = DAG.from_deps(deps)

mapping = exp.get_block_aliases_map()

print [mapping[n] for n in dag.L]

sr = ScopeRunner(exp, "root")
sr.execute()
