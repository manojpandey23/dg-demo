"""
Flow parser for DAG expressions.

Parses flow definitions like "asset1 >> asset2 << asset3" into dependency graphs.

Syntax:
- >> : downstream (left depends on right)
- << : upstream (right depends on left)
- [] : parallel execution (no dependency)
"""

from dataclasses import dataclass
from typing import List, Set, Dict


@dataclass
class DependencyNode:
    """Represents a node in the dependency graph"""
    name: str
    dependencies: List[str]  # Assets this depends on
    dependents: List[str]  # Assets that depend on this


class FlowParser:
    """Parse and validate DAG flow expressions"""

    # Regex patterns
    DOWNSTREAM_OP = ">>"
    UPSTREAM_OP = "<<"
    PARALLEL_START = "["
    PARALLEL_END = "]"

    @staticmethod
    def parse(flow_definition: str) -> Dict[str, DependencyNode]:
        """
        Parse a flow definition into a dependency graph.

        Operators:
            ``>>`` — downstream: right segment depends on left segment.
            ``<<`` — upstream: left segment depends on right segment.

        Examples:
            "raw_api >> raw_db >> stage_table"
            "raw_file, raw_api >> [raw_db, raw_cache] >> stage_table"
            "raw_api >> raw_db << stage_table"

        Returns:
            Dict mapping asset name to its DependencyNode.
        """
        flow = flow_definition.strip()

        segments, operators = FlowParser._split_segments(flow)

        # Build dependency graph
        nodes: Dict[str, DependencyNode] = {}

        for segment in segments:
            for asset in FlowParser._parse_segment(segment):
                if asset not in nodes:
                    nodes[asset] = DependencyNode(name=asset, dependencies=[], dependents=[])

        # Link dependencies according to operators
        for i, op in enumerate(operators):
            left_assets = FlowParser._parse_segment(segments[i])
            right_assets = FlowParser._parse_segment(segments[i + 1])

            if op == FlowParser.DOWNSTREAM_OP:
                # >> : right depends on left
                for right in right_assets:
                    for left in left_assets:
                        if left not in nodes[right].dependencies:
                            nodes[right].dependencies.append(left)
                        if right not in nodes[left].dependents:
                            nodes[left].dependents.append(right)

            elif op == FlowParser.UPSTREAM_OP:
                # << : left depends on right
                for left in left_assets:
                    for right in right_assets:
                        if right not in nodes[left].dependencies:
                            nodes[left].dependencies.append(right)
                        if left not in nodes[right].dependents:
                            nodes[right].dependents.append(left)

        FlowParser._validate_graph(nodes)
        return nodes

    @staticmethod
    def _split_segments(flow: str) -> tuple[List[str], List[str]]:
        """
        Split flow into segments and their connecting operators.

        Returns:
            A tuple of (segments, operators) where ``len(operators) == len(segments) - 1``.
        """
        import re

        tokens = re.split(r'\s*(>>|<<)\s*', flow)

        segments: List[str] = []
        operators: List[str] = []

        for idx, token in enumerate(tokens):
            token = token.strip()
            if not token:
                continue
            if idx % 2 == 0:
                segments.append(token)
            else:
                operators.append(token)

        if len(operators) != len(segments) - 1:
            raise ValueError(
                f"Malformed flow expression: expected {len(segments) - 1} operators, "
                f"got {len(operators)}"
            )

        return segments, operators

    @staticmethod
    def _parse_segment(segment: str) -> List[str]:
        """
        Parse a segment into asset names.
        
        Segments can be:
        - Single asset: "raw_api"
        - Parallel assets: "[raw_db, raw_cache]"
        - Comma-separated: "raw_api, raw_file"
        """
        segment = segment.strip()

        # Handle parallel notation [asset1, asset2]
        if segment.startswith("[") and segment.endswith("]"):
            inner = segment[1:-1]
            assets = [a.strip() for a in inner.split(",")]
            return assets

        # Handle comma-separated
        if "," in segment:
            return [a.strip() for a in segment.split(",")]

        # Single asset
        return [segment]

    @staticmethod
    def _validate_graph(nodes: Dict[str, DependencyNode]) -> None:
        """Validate the dependency graph for cycles"""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def has_cycle(node_name: str) -> bool:
            visited.add(node_name)
            rec_stack.add(node_name)

            node = nodes.get(node_name)
            if node:
                for dep in node.dependencies:
                    if dep not in visited:
                        if has_cycle(dep):
                            return True
                    elif dep in rec_stack:
                        return True

            rec_stack.remove(node_name)
            return False

        for node_name in nodes:
            if node_name not in visited:
                if has_cycle(node_name):
                    raise ValueError(f"Circular dependency detected involving {node_name}")

    @staticmethod
    def get_execution_order(nodes: Dict[str, DependencyNode]) -> List[str]:
        """
        Get topologically sorted execution order.
        
        Returns:
            List of asset names in execution order
        """
        visited: Set[str] = set()
        result: List[str] = []

        def visit(node_name: str):
            if node_name in visited:
                return

            visited.add(node_name)
            node = nodes.get(node_name)

            if node:
                for dep in node.dependencies:
                    visit(dep)

            result.append(node_name)

        for node_name in nodes:
            visit(node_name)

        return result
