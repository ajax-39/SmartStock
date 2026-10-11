# SmartStock — Big Data Stock Investment Recommendation System

A research-oriented stock investment decision-support system using historical Indian stock market data at one-minute frequency.

## Technologies

- **Big Data Processing**: Apache Spark, PySpark, Spark SQL
- **Machine Learning**: Spark MLlib
- **Data Storage**: Hadoop HDFS, Parquet
- **Application**: Python, Streamlit
- **Cloud**: Google Cloud Platform (Storage, Dataproc, Cloud Run)

## Project Structure

```
SmartStock/
├── data/                   # Data directory (gitignored)
│   ├── raw/               # Raw Kaggle data
│   └── processed/         # Processed Parquet files
├── src/                   # Source code
│   ├── data/             # Data loading and processing
│   ├── features/         # Feature engineering
│   ├── models/           # ML model training and evaluation
│   ├── backtesting/      # Backtesting engine
│   └── dashboard/        # Streamlit dashboard
├── notebooks/            # Jupyter notebooks for exploration
├── tests/               # Unit and integration tests
├── config/              # Configuration files
├── scripts/             # Utility scripts
├── deployment/          # GCP deployment configs
└── docs/               # Documentation

```

## Setup Instructions

### 1. Environment Setup

```bash
# Create virtual environment
python -m venv venv

# Activate (Windows PowerShell)
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2. Download Dataset

```bash
# Run from project root
python scripts/download_kaggle_data.py
```

### 3. Inspect Dataset

```bash
python scripts/inspect_dataset.py
```

## Development Phases

1. ✅ Project Setup and Data Discovery
2. ⏳ Data Cleaning and Transformation (PySpark)
3. ⏳ Feature Engineering
4. ⏳ Model Training and Evaluation
5. ⏳ Backtesting Engine
6. ⏳ Streamlit Dashboard
7. ⏳ Google Cloud Deployment

## Important Notes

- Predictions are experimental research outputs, not guaranteed investment advice
- Transaction costs and realistic constraints are included in backtesting
- Chronological validation is used to prevent look-ahead bias
- Test stocks are held out from training for unbiased evaluation

## Dataset

Indian stock market historical data at 1-minute frequency from Kaggle.
Downloaded using kagglehub API.

## License

Educational and research purposes only.
