"""
Centralized configuration management for SmartStock.

This module provides a unified interface for accessing configuration from:
- YAML files (config/config.yaml)
- Environment variables
- Command-line arguments (passed programmatically)

Environment variables take precedence over YAML config.
"""

import os
import yaml
from pathlib import Path
from typing import Any, Dict, Optional
from dataclasses import dataclass, field


@dataclass
class PathConfig:
    """Path configuration."""
    data_root: str
    raw_data: str
    processed_data: str
    cache: str
    models: str
    logs: str


@dataclass
class KaggleConfig:
    """Kaggle dataset configuration."""
    dataset_id: str
    expected_files: list = field(default_factory=list)


@dataclass
class SparkConfig:
    """Spark configuration."""
    app_name: str = "SmartStock"
    master: str = "local[*]"
    executor_memory: str = "4g"
    driver_memory: str = "2g"
    log_level: str = "WARN"


@dataclass
class DataConfig:
    """Data processing configuration."""
    test_stocks: list = field(default_factory=list)
    min_data_points: int = 10000
    train_end_date: Optional[str] = None
    test_start_date: Optional[str] = None


@dataclass
class IngestionConfig:
    """Data ingestion configuration."""
    # Dataset source path (can be overridden by env var or CLI)
    source_path: Optional[str] = None
    
    # Column mapping strategy
    auto_detect_columns: bool = True
    
    # Canonical column names (will be mapped from source)
    timestamp_col: str = "timestamp"
    symbol_col: str = "symbol"
    open_col: str = "open"
    high_col: str = "high"
    low_col: str = "low"
    close_col: str = "close"
    volume_col: str = "volume"
    
    # Data quality thresholds
    max_null_percentage: float = 10.0  # Reject columns with >10% nulls
    max_duplicate_percentage: float = 5.0  # Warn if >5% duplicates
    
    # Processing options
    chunk_size: int = 100000  # Process in chunks for memory efficiency
    validate_prices: bool = True  # Validate OHLC relationships
    remove_duplicates: bool = True
    
    # Partitioning & output
    output_format: str = "parquet"  # parquet or csv
    partition_by_symbol: bool = True  # Partition parquet by symbol for fast queries
    symbol_provenance: str = "auto"  # 'column', 'filename', or 'auto'
    max_files: Optional[int] = None  # Optional limit for testing/sampling


@dataclass
class GCPConfig:
    """Google Cloud Platform configuration."""
    project_id: str = ""
    region: str = "us-central1"
    bucket_name: str = ""
    dataproc_cluster: str = "smartstock-cluster"
    raw_data_uri: str = ""
    processed_data_uri: str = ""
    models_uri: str = ""

    def get_gcs_path(self, subpath: str) -> str:
        """Construct a gs:// URI if bucket is configured."""
        if self.bucket_name:
            clean_sub = subpath.lstrip('/')
            return f"gs://{self.bucket_name}/{clean_sub}"
        return ""


class Config:
    """
    Main configuration class.
    
    Loads configuration from YAML and allows override via environment variables.
    """
    
    def __init__(self, config_path: Optional[str] = None, override_source_path: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to YAML config file
            override_source_path: Override dataset source path
        """
        self.project_root = Path(__file__).parent.parent
        
        if config_path is None:
            config_path = self.project_root / "config" / "config.yaml"
        
        self.config_path = Path(config_path)
        self._raw_config = self._load_yaml()
        
        # Initialize configuration sections
        self.paths = self._init_paths()
        self.kaggle = self._init_kaggle()
        self.spark = self._init_spark()
        self.data = self._init_data()
        self.ingestion = self._init_ingestion(override_source_path)
        self.gcp = self._init_gcp()
    
    def _load_yaml(self) -> Dict[str, Any]:
        """Load YAML configuration file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _resolve_path(self, path: str) -> str:
        """Resolve relative path to absolute, or keep cloud URIs intact."""
        if not path:
            return ""
        
        # Preserve cloud storage and remote URIs
        if path.startswith(('gs://', 's3://', 'hdfs://', 'http://', 'https://')):
            return path
        
        path_obj = Path(path)
        if path_obj.is_absolute():
            return str(path_obj)
        else:
            return str(self.project_root / path)
    
    def _init_paths(self) -> PathConfig:
        """Initialize path configuration."""
        paths_config = self._raw_config.get('paths', {})
        
        return PathConfig(
            data_root=self._resolve_path(paths_config.get('data_root', 'data')),
            raw_data=self._resolve_path(
                os.getenv('SMARTSTOCK_RAW_DATA_PATH', paths_config.get('raw_data', 'data/raw'))
            ),
            processed_data=self._resolve_path(
                os.getenv('SMARTSTOCK_PROCESSED_DATA_PATH', paths_config.get('processed_data', 'data/processed'))
            ),
            cache=self._resolve_path(
                os.getenv('SMARTSTOCK_CACHE_PATH', paths_config.get('cache', 'data/cache'))
            ),
            models=self._resolve_path(paths_config.get('models', 'models/saved')),
            logs=self._resolve_path(paths_config.get('logs', 'logs'))
        )
    
    def _init_kaggle(self) -> KaggleConfig:
        """Initialize Kaggle configuration."""
        kaggle_config = self._raw_config.get('kaggle', {})
        
        return KaggleConfig(
            dataset_id=os.getenv('KAGGLE_DATASET_ID', kaggle_config.get('dataset_id', '')),
            expected_files=kaggle_config.get('expected_files', [])
        )
    
    def _init_spark(self) -> SparkConfig:
        """Initialize Spark configuration."""
        spark_config = self._raw_config.get('spark', {})
        
        return SparkConfig(
            app_name=os.getenv('SPARK_APP_NAME', spark_config.get('app_name', 'SmartStock')),
            master=os.getenv('SPARK_MASTER', spark_config.get('master', 'local[*]')),
            executor_memory=os.getenv('SPARK_EXECUTOR_MEMORY', spark_config.get('executor_memory', '4g')),
            driver_memory=os.getenv('SPARK_DRIVER_MEMORY', spark_config.get('driver_memory', '2g')),
            log_level=spark_config.get('log_level', 'WARN')
        )
    
    def _init_data(self) -> DataConfig:
        """Initialize data configuration."""
        data_config = self._raw_config.get('data', {})
        
        return DataConfig(
            test_stocks=data_config.get('test_stocks', []),
            min_data_points=int(os.getenv('MIN_DATA_POINTS', data_config.get('min_data_points', 10000))),
            train_end_date=data_config.get('train_end_date'),
            test_start_date=data_config.get('test_start_date')
        )
    
    def _resolve_source_path(self, override_source_path: Optional[str] = None) -> str:
        """
        Resolve data source path by priority:
        1. Explicit override argument (--path)
        2. SMARTSTOCK_DATASET_PATH environment variable
        3. YAML config ingestion.source_path
        4. Local data/raw if it contains data files
        5. Kagglehub cache directory if dataset was downloaded
        6. Default to raw_data path
        """
        # 1. Override argument
        if override_source_path:
            return override_source_path
            
        # 2. Environment variable
        env_path = os.getenv('SMARTSTOCK_DATASET_PATH')
        if env_path:
            return env_path
            
        # 3. YAML config
        ingestion_config = self._raw_config.get('ingestion', {})
        config_path = ingestion_config.get('source_path')
        if config_path:
            return self._resolve_path(config_path)
            
        # 4. Check if local raw_data has files
        raw_path = Path(self.paths.raw_data)
        if raw_path.exists():
            data_files = list(raw_path.glob('*.csv')) + list(raw_path.glob('*.parquet'))
            if data_files:
                return str(raw_path)
                
        # 5. Check kagglehub cache without redownloading
        dataset_id = self.kaggle.dataset_id
        if dataset_id:
            try:
                import kagglehub
                cached = kagglehub.dataset_download(dataset_id)
                if cached and Path(cached).exists():
                    return str(cached)
            except Exception:
                pass
                
        # 6. Fallback
        return self.paths.raw_data

    def _init_ingestion(self, override_source_path: Optional[str] = None) -> IngestionConfig:
        """Initialize ingestion configuration."""
        ingestion_config = self._raw_config.get('ingestion', {})
        source_path = self._resolve_source_path(override_source_path)
        
        return IngestionConfig(
            source_path=source_path,
            auto_detect_columns=ingestion_config.get('auto_detect_columns', True),
            timestamp_col=ingestion_config.get('timestamp_col', 'timestamp'),
            symbol_col=ingestion_config.get('symbol_col', 'symbol'),
            open_col=ingestion_config.get('open_col', 'open'),
            high_col=ingestion_config.get('high_col', 'high'),
            low_col=ingestion_config.get('low_col', 'low'),
            close_col=ingestion_config.get('close_col', 'close'),
            volume_col=ingestion_config.get('volume_col', 'volume'),
            max_null_percentage=float(ingestion_config.get('max_null_percentage', 10.0)),
            max_duplicate_percentage=float(ingestion_config.get('max_duplicate_percentage', 5.0)),
            chunk_size=int(os.getenv('INGESTION_CHUNK_SIZE', ingestion_config.get('chunk_size', 100000))),
            validate_prices=ingestion_config.get('validate_prices', True),
            remove_duplicates=ingestion_config.get('remove_duplicates', True),
            output_format=ingestion_config.get('output_format', 'parquet'),
            partition_by_symbol=ingestion_config.get('partition_by_symbol', True),
            symbol_provenance=ingestion_config.get('symbol_provenance', 'auto'),
            max_files=ingestion_config.get('max_files', None)
        )
    
    def _init_gcp(self) -> GCPConfig:
        """Initialize GCP configuration."""
        gcp_config = self._raw_config.get('gcp', {})
        bucket = os.getenv('GCP_BUCKET_NAME', gcp_config.get('bucket_name', ''))
        
        return GCPConfig(
            project_id=os.getenv('GCP_PROJECT_ID', gcp_config.get('project_id', '')),
            region=os.getenv('GCP_REGION', gcp_config.get('region', 'us-central1')),
            bucket_name=bucket,
            dataproc_cluster=os.getenv('DATAPROC_CLUSTER', gcp_config.get('dataproc_cluster', 'smartstock-cluster')),
            raw_data_uri=os.getenv('GCS_RAW_DATA_PATH', f"gs://{bucket}/raw" if bucket else ""),
            processed_data_uri=os.getenv('GCS_PROCESSED_DATA_PATH', f"gs://{bucket}/processed" if bucket else ""),
            models_uri=os.getenv('GCS_MODELS_PATH', f"gs://{bucket}/models" if bucket else "")
        )
    
    def get_nested(self, key: str, default: Any = None) -> Any:
        """
        Get nested configuration value using dot notation.
        
        Args:
            key: Dot-separated key (e.g., 'spark.app_name')
            default: Default value if not found
            
        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        value = self._raw_config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'paths': self.paths.__dict__,
            'kaggle': self.kaggle.__dict__,
            'spark': self.spark.__dict__,
            'data': self.data.__dict__,
            'ingestion': self.ingestion.__dict__,
            'gcp': self.gcp.__dict__
        }


# Singleton instance
_config_instance: Optional[Config] = None


def get_config(config_path: Optional[str] = None, 
               override_source_path: Optional[str] = None,
               force_reload: bool = False) -> Config:
    """
    Get singleton configuration instance.
    
    Args:
        config_path: Path to config file
        override_source_path: Override dataset source path
        force_reload: Force reload even if instance exists
        
    Returns:
        Config instance
    """
    global _config_instance
    
    if _config_instance is None or force_reload:
        _config_instance = Config(config_path, override_source_path)
    
    return _config_instance
