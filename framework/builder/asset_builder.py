# price_domain/framework/core/asset_builder.py
from typing import Callable, Optional

import dagster as dg

from framework.builder.dependency_builder import build_asset_dependencies
from framework.core.asserts.assert_registry import ASSERT_REGISTRY
from framework.model.config_models import AssertType, AssetConfig, FileFormatConfig


class AssetBuilder:
    """Factory for building Dagster assets from config"""

    _or_bridge_map: dict[str, str] = {}

    @staticmethod
    def build_assets(
        configs: list[AssetConfig],
        jobs_config,
        file_formatters: dict[str, FileFormatConfig] | None = None,
    ) -> list:
        asset_deps, or_groups = build_asset_dependencies(jobs_config)

        asset_deps, bridge_assets = AssetBuilder._apply_or_bridges(
            asset_deps, or_groups
        )

        # ✅ Record OR‑bridge mapping
        AssetBuilder._or_bridge_map = {
            asset: f"__or_bridge_{asset}" for asset in or_groups
        }

        assets = []
        assets.extend(bridge_assets)

        for config in configs:
            asset = AssetBuilder.build_asset(
                config, asset_deps, file_formatters=file_formatters
            )
            if asset:
                assets.append(asset)

        return assets

    @staticmethod
    def get_or_bridge_map() -> dict[str, str]:
        return dict(AssetBuilder._or_bridge_map)

    @staticmethod
    def _apply_or_bridges(
        deps: dict[str, list[dg.AssetKey]],
        or_groups: dict[str, list[dg.AssetKey]],
    ):
        """
        Rewrite dependencies to insert OR-bridge assets.
        """
        new_deps = dict(deps)
        bridge_assets: list[Callable] = []

        for asset_name, upstreams in or_groups.items():
            bridge_name = f"__or_bridge_{asset_name}"
            bridge_key = dg.AssetKey(bridge_name)

            # Replace upstreams with bridge
            new_deps[asset_name] = [bridge_key]

            # Build bridge asset
            bridge_assets.append(
                AssetBuilder._build_or_bridge_asset(asset_name, upstreams)
            )

        return new_deps, bridge_assets

    @staticmethod
    def build_asset(
        config: AssetConfig,
        asset_deps: dict[str, list[dg.AssetKey]],
        file_formatters: dict[str, FileFormatConfig] | None = None,
    ) -> Optional[Callable]:

        try:
            assert_type = AssertType(config.type)
        except ValueError:
            raise ValueError(f"Unknown asset type: {config.type}")

        handler = ASSERT_REGISTRY.get(assert_type)

        # File assets receive the formatter registry
        if assert_type == AssertType.file:
            return handler(config, asset_deps, file_formatters=file_formatters)

        return handler(config, asset_deps)

    @staticmethod
    def _build_or_bridge_asset(
        target_asset: str,
        upstreams: list[dg.AssetKey],
    ) -> Callable:
        """
        Build a synthetic OR-bridge asset.

        Name example:
            __or_bridge_cash_balance_stage
        """
        bridge_name = f"__or_bridge_{target_asset}"

        ins = {f"input_{i}": dg.AssetIn(key=key) for i, key in enumerate(upstreams)}

        @dg.asset(
            name=bridge_name,
            ins=ins,
            io_manager_key="noop_io_manager",
            tags={"synthetic": "true", "or_bridge": target_asset},
        )
        def or_bridge(**inputs):
            for df in inputs.values():
                if df is not None:
                    return df
            # If nothing was materialized in this run
            return None

        return or_bridge
