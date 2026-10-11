# SmartStock Setup Guide

## Prerequisites

### Required Software

1. **Python 3.9+**
   - Download from [python.org](https://www.python.org/downloads/)
   - Verify: `python --version`

2. **Java 8 or 11** (required for Spark)
   - Download from [Oracle](https://www.oracle.com/java/technologies/downloads/) or use OpenJDK
   - Verify: `java -version`
   - Set `JAVA_HOME` environment variable

3. **Apache Spark 3.5+** (optional for local development)
   - Will be installed via PySpark, but having native Spark is recommended
   - Download from [spark.apache.org](https://spark.apache.org/downloads.html)

4. **Git**
   - Download from [git-scm.com](https://git-scm.com/downloads/)
   - Verify: `git --version`

### Kaggle API Setup

1. Create a Kaggle account at [kaggle.com](https://www.kaggle.com)

2. Generate API credentials:
   - Go to Kaggle → Account → API → Create New API Token
   - This downloads `kaggle.json`

3. Place credentials in the correct location:
   - **Windows**: `C:\Users\<YourUsername>\.kaggle\kaggle.json`
   - **Linux/Mac**: `~/.kaggle/kaggle.json`

4. Set permissions (Linux/Mac only):
   ```bash
   chmod 600 ~/.kaggle/kaggle.json
   ```

## Installation Steps

### 1. Clone or Initialize Repository

```powershell
# If starting fresh (already done)
cd Z:\Sem-7\BDA\SmartStock
git init
```

### 2. Create Virtual Environment

```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# If execution policy error, run:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 3. Install Dependencies

```powershell
# Upgrade pip
python -m pip install --upgrade pip

# Install requirements
pip install -r requirements.txt
```

**Note**: This will take several minutes as PySpark and dependencies are large.

### 4. Verify Installation

```powershell
# Test Python imports
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
python -c "import pandas; import numpy; print('Core packages OK')"

# Test configuration loader
python -c "from src.config_loader import get_config; c = get_config(); print('Config OK')"
```

### 5. Download Dataset

```powershell
# Download from Kaggle (this may take 10-30 minutes)
python scripts/download_kaggle_data.py
```

### 6. Inspect Dataset

```powershell
# Analyze the downloaded data
python scripts/inspect_dataset.py
```

This will display:
- File structure
- Column schema
- Number of stocks
- Date range
- Sample data

## Directory Structure After Setup

```
SmartStock/
├── venv/                      # Virtual environment (not in git)
├── data/
│   ├── raw/                  # Downloaded Kaggle data
│   │   └── [CSV files]
│   ├── processed/            # Will be created during processing
│   └── cache/
│       └── dataset_inspection.json
├── config/
│   └── config.yaml           # Main configuration
├── src/
│   ├── __init__.py
│   ├── config_loader.py
│   └── [module directories]
├── scripts/
│   ├── download_kaggle_data.py
│   └── inspect_dataset.py
├── tests/
│   └── test_config_loader.py
└── [other project files]
```

## Running Tests

```powershell
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_config_loader.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Troubleshooting

### PySpark Installation Issues

**Issue**: Java not found
```
Solution: Install Java 8 or 11 and set JAVA_HOME environment variable
Windows: setx JAVA_HOME "C:\Program Files\Java\jdk-11"
```

**Issue**: Out of memory errors
```
Solution: Increase Spark memory in config/config.yaml:
spark:
  executor_memory: "2g"  # Reduce if needed
  driver_memory: "1g"
```

### Kaggle Download Issues

**Issue**: Kaggle API authentication failed
```
Solution: 
1. Verify kaggle.json is in correct location
2. Check file contains valid credentials
3. Accept dataset terms on Kaggle website
```

**Issue**: Dataset not found
```
Solution: Verify dataset ID in config/config.yaml matches Kaggle URL
```

### Import Errors

**Issue**: ModuleNotFoundError
```
Solution: 
1. Ensure virtual environment is activated
2. Reinstall requirements: pip install -r requirements.txt
3. Check Python path includes project root
```

## Next Steps

After successful setup:

1. ✅ Review dataset inspection results
2. ✅ Update `config/config.yaml` with:
   - Test stocks to hold out
   - Date ranges for train/test split
3. ⏩ Proceed to Phase 2: Data Cleaning and Transformation

## Getting Help

- Check existing error messages in console output
- Review configuration in `config/config.yaml`
- Inspect dataset summary in `data/cache/dataset_inspection.json`
- Consult PySpark documentation: [spark.apache.org/docs/latest/api/python/](https://spark.apache.org/docs/latest/api/python/)
