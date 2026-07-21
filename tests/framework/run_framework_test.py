"""Test framework loading with jobs, sensors, and validations."""

from pathlib import Path

import pytest

from framework import FrameworkLoader


@pytest.fixture
def config_dir():
    return Path(__file__).resolve().parents[2] / "src" / "test_domain" / "configs"


def test_framework_loads_all_components(config_dir):
    if not config_dir.exists():
        pytest.skip("test_domain configs not found")

    loader = FrameworkLoader(config_dir=config_dir, environment="local")
    defs = loader.get_definitions()

    assert defs is not None
    assert len(loader.resources) > 0
