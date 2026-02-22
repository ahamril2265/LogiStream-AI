"""Pytest configuration."""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


@pytest.fixture(scope="session")
def config():
    """Provide config for tests."""
    from config import get_config
    return get_config()


@pytest.fixture
def sample_dataframe():
    """Provide sample DataFrame for tests."""
    import pandas as pd
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "value": [10.0, 20.0, 30.0, 40.0, 50.0],
        "timestamp": pd.date_range("2024-01-01", periods=5),
    })
