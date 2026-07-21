"""Shared test fixtures for the framework test suite."""

from pathlib import Path

import pandas as pd
import pytest

from framework.model.config_models import (
    AssetConfig,
    AssetSchema,
    AssertType,
    DatabaseModelConfig,
    FileFormatConfig,
    FileFormatType,
    FrameworkPipelineConfig,
    JobConfig,
    JobFlow,
    Materialization,
    SensorConfig,
    SensorTriggerConfig,
    SensorType,
    TransformConfig,
)

TEST_FILES_DIR = Path(__file__).parent / "framework" / "test_files"


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "amount": [100.50, 200.75, 50.00, 300.25, 150.00],
            "currency": ["USD", "EUR", "GBP", "USD", "EUR"],
            "date": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
            ),
        }
    )


@pytest.fixture
def nullable_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "amount": [100.0, None, 50.0],
            "currency": ["USD", "EUR", None],
        }
    )


@pytest.fixture
def empty_df():
    return pd.DataFrame(columns=["id", "name", "amount"])


@pytest.fixture
def sample_schema():
    return [
        AssetSchema(name="id", dtype="int", nullable=False, isKey=True),
        AssetSchema(name="name", dtype="string", nullable=False),
        AssetSchema(name="amount", dtype="float", nullable=True),
    ]


@pytest.fixture
def api_asset_config():
    return AssetConfig(
        name="test_api_asset",
        type=AssertType.api,
        description="Test API asset",
        group_name="test",
        source={"resource": "api_resource", "endpoint": "/test", "method": "GET"},
        columns=[
            AssetSchema(name="id", dtype="int", nullable=False),
            AssetSchema(name="value", dtype="float"),
        ],
    )


@pytest.fixture
def db_asset_config():
    return AssetConfig(
        name="test_db_asset",
        type=AssertType.database,
        description="Test DB asset",
        group_name="test",
        source={"resource": "postgres_resource", "table": "test.test_table"},
        model=DatabaseModelConfig(materialization=Materialization.table),
        columns=[
            AssetSchema(name="id", dtype="int", nullable=False, isKey=True),
            AssetSchema(name="name", dtype="string"),
            AssetSchema(name="amount", dtype="float"),
        ],
    )


@pytest.fixture
def file_asset_config():
    return AssetConfig(
        name="test_file_asset",
        type=AssertType.file,
        description="Test file asset",
        group_name="test",
        partition_name="test_files",
    )


@pytest.fixture
def sample_job_config():
    return JobConfig(
        name="test_pipeline",
        description="Test pipeline",
        flow=JobFlow(
            definition="asset_a >> asset_b >> asset_c",
            description="Sequential flow",
        ),
    )


@pytest.fixture
def sample_sensor_config():
    return SensorConfig(
        name="test_sensor",
        type=SensorType.schedule,
        description="Test sensor",
        trigger=SensorTriggerConfig(
            type="job",
            target="test_pipeline",
            cron="0 * * * *",
        ),
    )


@pytest.fixture
def sample_pipeline_config():
    return FrameworkPipelineConfig(
        version="1.0",
        name="test_pipeline",
        description="Test pipeline config",
        assets=[
            AssetConfig(
                name="source_asset",
                type=AssertType.api,
                source={"resource": "api_resource", "endpoint": "/data"},
            ),
        ],
        jobs=[
            JobConfig(
                name="test_job",
                flow=JobFlow(definition="source_asset"),
            ),
        ],
    )


@pytest.fixture
def csv_file_format():
    return FileFormatConfig(
        name="pipe_csv",
        type=FileFormatType.csv,
        config={"delimiter": "|", "encoding": "utf-8"},
    )


@pytest.fixture
def transform_config():
    return TransformConfig(
        pre='frame.filter(col("amount") > 0)',
        post='frame.order_by(desc("amount")).limit(10)',
    )


@pytest.fixture
def test_csv_path():
    return str(TEST_FILES_DIR / "test_data.csv")


@pytest.fixture
def test_json_path():
    return str(TEST_FILES_DIR / "test_data.json")
