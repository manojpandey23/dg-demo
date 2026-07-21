"""Test sensor and job loading."""

from pathlib import Path

import pytest

from framework import FrameworkLoader


@pytest.fixture
def config_dir():
    return Path(__file__).resolve().parents[2] / "demo" / "configs"


def test_framework_loader_discovers_configs(config_dir):
    if not config_dir.exists():
        pytest.skip("demo configs not found")

    loader = FrameworkLoader(config_dir=config_dir, environment="local")
    defs = loader.get_definitions()
    assert defs is not None
