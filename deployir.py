from dataclasses import dataclass
from typing import Any


@dataclass
class Constraint:
    """
    Deploy Ir 中间表示,统一约束
    """
    source:str #"来源 app1"
    target:str #"来源 app2"
    operator:str #"操作符  requires excludes"
    type:str # "约束类型 pod affinity / pod anti affinity"
    tracing_info:dict[str,Any]

    def __str__(self):
        return f"[{self.tracing_info['file']}] {self.source} {self.operator} {self.target} (Type: {self.type})"
