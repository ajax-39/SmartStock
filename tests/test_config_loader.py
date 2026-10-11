"""Tests for configuration loader."""

import pytest
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config_loader import ConfigLoader, get_config


def test_config_loader_initialization():
    """Test that configuration loader initializes correctly."""
    config = ConfigLoader()
    assert config.config is not None
    assert isinstance(config.config, dict)


def test_config_get():
    """Test getting configuration values."""
    config = ConfigLoader()
    
    # Test getting nested value
    app_name = config.get('spark.app_name')
    assert app_name == 'SmartStock'
    
    # Test getting non-existent value with default
    non_existent = config.get('nonexistent.key', 'default_value')
    assert non_existent == 'default_value'


def test_config_set():
    """Test setting configuration values."""
    config = ConfigLoader()
    
    # Set a new value
    config.set('test.key', 'test_value')
    assert config.get('test.key') == 'test_value'


def test_config_dictionary_access():
    """Test dictionary-style access."""
    config = ConfigLoader()
    
    # Test getting
    assert config['spark.app_name'] == 'SmartStock'
    
    # Test setting
    config['test.dict.access'] = 'value'
    assert config['test.dict.access'] == 'value'


def test_singleton_instance():
    """Test that get_config returns singleton instance."""
    config1 = get_config()
    config2 = get_config()
    assert config1 is config2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
