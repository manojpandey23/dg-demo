"""Test framework loader initialization and resource loading."""

from pathlib import Path

import pytest

from framework import FrameworkLoader


@pytest.fixture
def config_dir():
    return Path(__file__).resolve().parents[2] / "src" / "test_domain" / "configs"


def test_loader_initializes(config_dir):
    if not config_dir.exists():
        pytest.skip("test_domain configs not found")

    loader = FrameworkLoader(config_dir=config_dir, environment="local")
    assert len(loader.resources) > 0
    assert loader.describe_resources() is not None
