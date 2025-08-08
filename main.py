import z3
from typing import List, Dict, Tuple
from transformer import K8sTransformer
from deployir import Constraint


class SolverEngine:
    def __init__(self):
        self.solver = z3.Solver()
        self.app_vars: Dict[str, z3.BoolRef] = {}
        self.constraint_map: Dict[str, Constraint] = {}

    def _get_app_var(self, app_name: str) -> z3.BoolRef:
        if app_name not in self.app_vars:
            self.app_vars[app_name] = z3.Bool(app_name)
        return self.app_vars[app_name]

    def formalize_and_add(self, constraints: List[Constraint]):
        for i, c in enumerate(constraints):
            source_var = self._get_app_var(c.source)
            target_var = self._get_app_var(c.target)

            # 创建 Z3 逻辑表达式
            if c.operator == 'requires':
                logic_expr = z3.Implies(source_var, target_var)
            elif c.operator == 'excludes':
                logic_expr = z3.Not(z3.And(source_var, target_var))
            else:
                continue

            # 使用 z3.assert_and_track 来关联逻辑和原始约束
            tracker_var = z3.Bool(f"tracker_{i}")
            self.solver.assert_and_track(logic_expr, tracker_var)
            self.constraint_map[f"tracker_{i}"] = c

    def analyze_deployment(self, app_to_deploy: str) -> Tuple[str, List[Constraint]]:
        """分析部署单个应用时是否存在冲突。"""
        # 我们假设要部署这个应用，即其变量为 true
        app_var = self._get_app_var(app_to_deploy)

        # 使用上下文管理器来临时添加假设
        self.solver.push()
        self.solver.add(app_var == True)

        result = self.solver.check()

        unsat_core_constraints = []
        if result == z3.unsat:
            core = self.solver.unsat_core()
            for tracker in core:
                unsat_core_constraints.append(self.constraint_map[str(tracker)])

        self.solver.pop()  # 撤销假设
        return str(result), unsat_core_constraints


class DeployFix:

    def __init__(self):
        self.transformer = K8sTransformer()
        self.engine = SolverEngine()

    def analyze(self, file_paths: List[str]):
        print("--- 1. Translation Phase ---")
        all_constraints = []
        for file_path in file_paths:
            print(f"Transforming {file_path}...")
            constraints = self.transformer.transform(file_path)
            all_constraints.extend(constraints)

        print("\nDiscovered Constraints (DeployIR):")
        for c in all_constraints:
            print(f"  - {c}")

        print("\n--- 2. Formalization & Solving Phase ---")
        self.engine.formalize_and_add(all_constraints)

        all_apps = sorted(list(self.engine.app_vars.keys()))

        for app in all_apps:
            print(f"\nAnalyzing deployment of '{app}':")
            status, core = self.engine.analyze_deployment(app)

            print(f"  Result: {status.upper()}")
            if status == 'unsat':
                print("  Conflict Detected! The following constraints form a Minimal Unsatisfiable Core (MUC):")
                for c in core:
                    print(f"    - {c}")


if __name__ == "__main__":
    yaml_files = ['app1.yaml', 'app2.yaml']

    # 检查文件是否存在
    import os

    if not all(os.path.exists(f) for f in yaml_files):
        print(f"Error: Please make sure '{yaml_files[0]}' and '{yaml_files[1]}' are in the same directory.")
    else:
        deploy_fix_app = DeployFix()
        deploy_fix_app.analyze(yaml_files)