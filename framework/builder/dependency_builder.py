from collections import defaultdict

import dagster as dg

from framework.builder.flow_parser import FlowParser


def build_asset_dependencies(jobs_config):
    """
    Build merged asset dependencies AND detect OR semantics.

    Returns:
        deps: dict[str, list[AssetKey]]
        or_groups: dict[str, list[AssetKey]]
    """
    deps: dict[str, set[dg.AssetKey]] = defaultdict(set)

    # Track per-job dependencies
    per_job_deps: dict[str, list[set[str]]] = defaultdict(list)

    for job in jobs_config:
        nodes = FlowParser.parse(job.flow.definition)

        for asset_name, node in nodes.items():
            if not node.dependencies:
                continue

            dep_set = set(node.dependencies)
            per_job_deps[asset_name].append(dep_set)

            for dep in dep_set:
                deps[asset_name].add(dg.AssetKey(dep))

    final_deps: dict[str, list[dg.AssetKey]] = {}
    or_groups: dict[str, list[dg.AssetKey]] = {}

    for asset_name, upstream_keys in deps.items():
        upstream_list = sorted(
            upstream_keys,
            key=lambda k: k.to_user_string(),
        )

        final_deps[asset_name] = upstream_list

        # -------- OR detection --------
        job_sets = per_job_deps.get(asset_name, [])
        if len(job_sets) <= 1:
            continue

        # If no job contains more than one upstream → OR
        is_or = all(len(job_set) == 1 for job_set in job_sets)

        if is_or and len(upstream_list) > 1:
            or_groups[asset_name] = upstream_list

    return final_deps, or_groups
