"""Test YAML-driven resources system."""

from pathlib import Path

import pytest

from framework import FrameworkLoader


@pytest.fixture
def config_dir():
    return Path(__file__).resolve().parents[2] / "src" / "test_domain" / "configs"


def test_yaml_resources_load(config_dir):
    if not config_dir.exists():
        pytest.skip("test_domain configs not found")

    loader = FrameworkLoader(config_dir=config_dir, environment="local")
    assert "noop_io_manager" in loader.resources
