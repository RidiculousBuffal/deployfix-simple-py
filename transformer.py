# transformer.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any

import yaml

from deployir import Constraint


class AffinityParserStrategy(ABC):
    """
    解析 'affinity' 规则的抽象策略基类。
    """

    @abstractmethod
    def parse(self, affinity_spec: Dict[str, Any], source_app: str, file_name: str) -> List[Constraint]:
        pass


class PodAffinityParser(AffinityParserStrategy):
    """
    解析 podAffinity 的具体策略。
    podAffinity 通常表示 'requires' 关系。
    """

    def parse(self, affinity_spec: Dict[str, Any], source_app: str, file_name: str) -> List[Constraint]:
        constraints = []
        if 'podAffinity' in affinity_spec and 'requiredDuringSchedulingIgnoredDuringExecution' in affinity_spec[
            'podAffinity']:
            for rule in affinity_spec['podAffinity']['requiredDuringSchedulingIgnoredDuringExecution']:
                for expr in rule['labelSelector']['matchExpressions']:
                    if expr['operator'] == 'In':
                        for target_app in expr['values']:
                            constraints.append(Constraint(
                                source=source_app,
                                target=target_app,
                                operator='requires',
                                type='pod_affinity',
                                tracing_info={'file': file_name}
                            ))
        return constraints


class PodAntiAffinityParser(AffinityParserStrategy):
    """
    解析 podAntiAffinity 的具体策略。
    podAntiAffinity 通常表示 'excludes' 关系。
    """

    def parse(self, affinity_spec: Dict[str, Any], source_app: str, file_name: str) -> List[Constraint]:
        constraints = []
        if 'podAntiAffinity' in affinity_spec and 'requiredDuringSchedulingIgnoredDuringExecution' in affinity_spec[
            'podAntiAffinity']:
            for rule in affinity_spec['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution']:
                for expr in rule['labelSelector']['matchExpressions']:
                    if expr['operator'] == 'In':
                        for target_app in expr['values']:
                            constraints.append(Constraint(
                                source=source_app,
                                target=target_app,
                                operator='excludes',
                                type='pod_anti_affinity',
                                tracing_info={'file': file_name}
                            ))
        return constraints


class NodeAffinityParser(AffinityParserStrategy):
    """
    解析 nodeAffinity 的具体策略。
    它将应用与节点标签之间的'In'(亲和)和'NotIn'(反亲和)关系转换为约束。
    """

    def parse(self, affinity_spec: Dict[str, Any], source_app: str, file_name: str) -> List[Constraint]:
        constraints = []
        # 我们只处理硬性要求
        if 'nodeAffinity' in affinity_spec and 'requiredDuringSchedulingIgnoredDuringExecution' in affinity_spec[
            'nodeAffinity']:
            for term in affinity_spec['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution'][
                'nodeSelectorTerms']:
                for expr in term['matchExpressions']:
                    # 将节点标签视为一个实体
                    if 'values' in expr:  # In 和 NotIn 操作符需要 values
                        for value in expr['values']:
                            node_label_entity = f"{expr['key']}={value}"

                            if expr['operator'] == 'In':
                                constraints.append(Constraint(
                                    source=source_app,
                                    target=node_label_entity,
                                    operator='requires',
                                    type='node_affinity',
                                    tracing_info={'file': file_name}
                                ))
                            elif expr['operator'] == 'NotIn':
                                constraints.append(Constraint(
                                    source=source_app,
                                    target=node_label_entity,
                                    operator='excludes',
                                    type='node_anti_affinity',  # 类型可以更精确
                                    tracing_info={'file': file_name}
                                ))
        return constraints


class K8sTransformer:
    """
    K8s YAML 转换器，它使用不同的策略来解析 'affinity'。
    """

    def __init__(self):
        self._strategies: List[AffinityParserStrategy] = [
            PodAffinityParser(),
            PodAntiAffinityParser(),
            NodeAffinityParser(),

        ]

    def transform(self, file_path: str) -> List[Constraint]:
        """
        读取单个 YAML 文件并将其转换为 Constraint 列表。
        """
        all_constraints = []
        with open(file_path, 'r') as f:
            docs = yaml.safe_load_all(f)
            for doc in docs:
                if doc.get('kind') == 'Deployment':
                    try:
                        # 从模板标签中获取源应用名称
                        source_app = doc['spec']['template']['metadata']['labels']['app']
                        affinity_spec = doc['spec']['template']['spec'].get('affinity', {})

                        # 应用所有策略
                        for strategy in self._strategies:
                            all_constraints.extend(strategy.parse(affinity_spec, source_app, file_path))
                    except (KeyError, TypeError):
                        # 忽略格式不正确的 YAML 部分
                        continue
        return all_constraints
