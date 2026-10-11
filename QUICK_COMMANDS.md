# SmartStock - Quick Command Reference

## Setup (One Time)

```powershell
# 1. Create virtual environment
python -m venv venv

# 2. Activate
.\venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requirements.txt
```

## Dataset Download & Inspection

```powershell
# Option A: Find existing kagglehub cache
python scripts/find_kagglehub_cache.py

# Option B: Download to project
python scripts/download_kaggle_data.py

# Inspect dataset (use appropriate path)
python src/ingestion/inspect_dataset.py
python src/ingestion/inspect_dataset.py --path "C:\Users\<User>\.cache\kagglehub\..."
python src/ingestion/inspect_dataset.py --path <custom-path> --output report.json
```

## Testing

```powershell
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_dataset_inspector.py -v
pytest tests/test_config_loader.py -v

# With coverage
pytest tests/ --cov=src --cov-report=html
```

## Common Tasks

```powershell
# View config
python -c "from src.config_loader import get_config; c = get_config(); print(c.config)"

# Check Python packages
pip list

# Update requirements
pip freeze > requirements.txt
```

## File Locations

| Item | Path |
|------|------|
| **Inspection report** | `data/cache/dataset_inspection_detailed.json` |
| **Config** | `config/config.yaml` |
| **Downloaded data** | `data/raw/` |
| **Kagglehub cache** | `C:\Users\<User>\.cache\kagglehub\...` |

## Help

```powershell
# Script help
python src/ingestion/inspect_dataset.py --help
python scripts/find_kagglehub_cache.py --help

# View docs
# - README.md              (overview)
# - QUICKSTART.md          (quick start)
# - docs/SETUP.md          (detailed setup)
# - docs/INSPECTION_GUIDE.md  (inspection details)
# - INSPECTION_TASK_COMPLETE.md  (current task status)
```

## Deactivate

```powershell
# When done working
deactivate
```
