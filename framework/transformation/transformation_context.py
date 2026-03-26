from collections import defaultdict
from typing import Dict, Set


class TransformationContext:
    """
    Holds runtime information for a single transformation execution.
    """

    def __init__(self, upstream_asset_key):
        self.upstream_asset_key = upstream_asset_key
        self.column_lineage: Dict[str, Set[str]] = defaultdict(set)

    def record_ref(self, output_column: str, input_column: str):
        self.column_lineage[output_column].add(input_column)