# SmartStock Quick Start Guide

Get started with SmartStock in 5 minutes.

## Prerequisites Check

```powershell
# Check Python (need 3.9+)
python --version

# Check Java (need 8 or 11)
java -version

# Check Kaggle credentials exist
Test-Path "$env:USERPROFILE\.kaggle\kaggle.json"
```

## Setup (First Time Only)

```powershell
# 1. Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# 2. Install dependencies (takes 5-10 minutes)
pip install -r requirements.txt

# 3. Download dataset (takes 10-30 minutes)
python scripts/download_kaggle_data.py

# 4. Inspect dataset
python scripts/inspect_dataset.py
```

## Daily Workflow

```powershell
# Activate environment
.\venv\Scripts\Activate.ps1

# Your work here...

# Run tests
pytest tests/ -v

# Deactivate when done
deactivate
```

## Project Phases

- ✅ **Phase 1**: Project Setup and Data Discovery ← YOU ARE HERE
- ⏳ **Phase 2**: Data Cleaning and Transformation (PySpark)
- ⏳ **Phase 3**: Feature Engineering
- ⏳ **Phase 4**: Model Training and Evaluation
- ⏳ **Phase 5**: Backtesting Engine
- ⏳ **Phase 6**: Streamlit Dashboard
- ⏳ **Phase 7**: Google Cloud Deployment

## Key Files

- `config/config.yaml` - Main configuration
- `data/cache/dataset_inspection.json` - Dataset analysis results
- `scripts/download_kaggle_data.py` - Dataset downloader
- `scripts/inspect_dataset.py` - Dataset inspector

## Common Commands

```powershell
# Test configuration
python -c "from src.config_loader import get_config; print(get_config()['spark.app_name'])"

# Re-inspect dataset
python scripts/inspect_dataset.py

# Run specific test
pytest tests/test_config_loader.py -v
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `java: command not found` | Install Java 8 or 11, set JAVA_HOME |
| Kaggle auth error | Place kaggle.json in `~/.kaggle/` |
| Module not found | Activate venv: `.\venv\Scripts\Activate.ps1` |
| Out of memory | Reduce Spark memory in config.yaml |

## Next Steps

After Phase 1 setup:
1. Review `data/cache/dataset_inspection.json`
2. Update test stocks in `config/config.yaml`
3. Ready for Phase 2: Data Processing!

For detailed setup, see `docs/SETUP.md`
