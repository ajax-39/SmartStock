"""Configuration loader utility for SmartStock project."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any


class ConfigLoader:
    """Load and manage configuration from YAML files."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize configuration loader.
        
        Args:
            config_path: Path to configuration file. If None, uses default.
        """
        if config_path is None:
            # Default to config/config.yaml relative to project root
            project_root = Path(__file__).parent.parent
            config_path = project_root / "config" / "config.yaml"
        
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._resolve_paths()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config
    
    def _resolve_paths(self):
        """Resolve relative paths to absolute paths."""
        project_root = Path(__file__).parent.parent
        
        if 'paths' in self.config:
            for key, value in self.config['paths'].items():
                if value and not Path(value).is_absolute():
                    self.config['paths'][key] = str(project_root / value)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'spark.app_name')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """
        Set configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'spark.app_name')
            value: Value to set
        """
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save(self, output_path: str = None):
        """
        Save configuration to YAML file.
        
        Args:
            output_path: Path to save configuration. If None, overwrites original.
        """
        if output_path is None:
            output_path = self.config_path
        
        with open(output_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
    
    def __getitem__(self, key: str) -> Any:
        """Allow dictionary-style access."""
        return self.get(key)
    
    def __setitem__(self, key: str, value: Any):
        """Allow dictionary-style assignment."""
        self.set(key, value)


# Singleton instance
_config_instance = None


def get_config(config_path: str = None) -> ConfigLoader:
    """
    Get singleton configuration instance.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        ConfigLoader instance
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = ConfigLoader(config_path)
    
    return _config_instance
