"""Tests for centralized configuration module."""

import pytest
import os
from pathlib import Path
import tempfile
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import get_config, Config


class TestConfig:
    """Test suite for Config class."""
    
    def test_config_initialization(self):
        """Test configuration initialization."""
        config = get_config(force_reload=True)
        assert config is not None
        assert config.spark.app_name == "SmartStock"
    
    def test_path_resolution(self):
        """Test path resolution to absolute paths."""
        config = get_config(force_reload=True)
        assert Path(config.paths.data_root).is_absolute()
        assert Path(config.paths.raw_data).is_absolute()
    
    def test_environment_variable_override(self):
        """Test that environment variables override YAML config."""
        # Set environment variable
        os.environ['SPARK_APP_NAME'] = 'TestApp'
        
        config = get_config(force_reload=True)
        assert config.spark.app_name == 'TestApp'
        
        # Clean up
        del os.environ['SPARK_APP_NAME']
    
    def test_dataset_path_override(self):
        """Test dataset path can be overridden."""
        override_path = "/custom/path/to/data"
        config = get_config(override_source_path=override_path, force_reload=True)
        assert config.ingestion.source_path == override_path
    
    def test_nested_get(self):
        """Test getting nested configuration values."""
        config = get_config(force_reload=True)
        app_name = config.get_nested('spark.app_name')
        assert app_name == 'SmartStock'
    
    def test_to_dict(self):
        """Test converting configuration to dictionary."""
        config = get_config(force_reload=True)
        config_dict = config.to_dict()
        assert 'paths' in config_dict
        assert 'spark' in config_dict
        assert 'ingestion' in config_dict
    
    def test_ingestion_config_defaults(self):
        """Test ingestion configuration defaults."""
        config = get_config(force_reload=True)
        assert config.ingestion.auto_detect_columns == True
        assert config.ingestion.timestamp_col == 'timestamp'
        assert config.ingestion.output_format == 'parquet'


def test_singleton_pattern():
    """Test that get_config returns singleton."""
    config1 = get_config()
    config2 = get_config()
    assert config1 is config2


def test_force_reload():
    """Test force reload creates new instance."""
    config1 = get_config()
    config2 = get_config(force_reload=True)
    # They should be different instances after force reload
    assert config1 is not config2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
