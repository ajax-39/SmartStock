# SmartStock System Architecture

## Overview

SmartStock is a big data-based stock investment recommendation system that processes historical Indian stock market data at 1-minute frequency using Apache Spark and machine learning.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     Data Sources                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Kaggle: Indian Stock Market 1-min Data             │  │
│  │  (Historical NIFTY 50 stocks)                        │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Processing Layer (PySpark)                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ Data Loading │→ │   Cleaning   │→ │Feature Eng.  │    │
│  │   (Spark)    │  │ Validation   │  │ (Spark SQL)  │    │
│  └──────────────┘  └──────────────┘  └──────────────┘    │
│                                              │              │
│                                              ▼              │
│                                    ┌──────────────────┐    │
│                                    │  Parquet Files   │    │
│                                    │  (HDFS/Local)    │    │
│                                    └──────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│           Machine Learning Layer (Spark MLlib)              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  Train/Test  │→ │  Model       │→ │  Evaluation  │    │
│  │  Split       │  │  Training    │  │  Metrics     │    │
│  │(Chronologic) │  │(RandomForest)│  │              │    │
│  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Backtesting & Evaluation                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  • Transaction costs                                 │  │
│  │  • Position sizing                                   │  │
│  │  • Stop loss / Take profit                          │  │
│  │  • Benchmark comparison                             │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                Application Layer                            │
│  ┌──────────────┐                  ┌──────────────┐        │
│  │  Streamlit   │                  │   FastAPI    │        │
│  │  Dashboard   │                  │  (Optional)  │        │
│  │              │                  │              │        │
│  │ • Recs       │                  │ • Predictions│        │
│  │ • Charts     │                  │ • Model API  │        │
│  │ • Backtest   │                  │              │        │
│  └──────────────┘                  └──────────────┘        │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│               Google Cloud Platform                         │
│  ┌───────────┐  ┌───────────┐  ┌───────────────┐          │
│  │    GCS    │  │ Dataproc  │  │  Cloud Run    │          │
│  │  Storage  │  │  Spark    │  │  Dashboard    │          │
│  └───────────┘  └───────────┘  └───────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Data Processing Layer

**Technology**: PySpark, Spark SQL, Hadoop HDFS

**Responsibilities**:
- Load raw CSV data from Kaggle
- Clean and validate data (handle missing values, outliers)
- Chronological ordering
- Store processed data in Parquet format

**Key Modules**:
- `src/data/loader.py` - Data loading utilities
- `src/data/cleaner.py` - Data cleaning and validation
- `src/data/processor.py` - Main PySpark processing pipeline

### 2. Feature Engineering Layer

**Technology**: PySpark, Spark SQL

**Responsibilities**:
- Calculate technical indicators (price, momentum, volatility, volume)
- Generate time-based features
- Create target variable (future return probability)
- Avoid look-ahead bias

**Features**:
- **Price Features**: Returns over multiple windows
- **Momentum**: Rate of change, RSI-like features
- **Volatility**: Standard deviation over windows
- **Volume**: Volume changes and ratios

**Key Modules**:
- `src/features/technical_indicators.py`
- `src/features/feature_pipeline.py`

### 3. Machine Learning Layer

**Technology**: Spark MLlib

**Responsibilities**:
- Split data chronologically (no random shuffling)
- Train classification model (Random Forest)
- Evaluate on held-out test stocks
- Generate predictions

**Model**:
- **Algorithm**: Random Forest Classifier (Spark MLlib)
- **Target**: Binary classification (favorable return > threshold)
- **Features**: Technical indicators from feature layer
- **Validation**: Chronological split + held-out stocks

**Key Modules**:
- `src/models/trainer.py`
- `src/models/evaluator.py`
- `src/models/predictor.py`

### 4. Backtesting Layer

**Technology**: Python, Pandas

**Responsibilities**:
- Simulate realistic trading scenarios
- Apply transaction costs
- Implement position sizing and risk management
- Compare against benchmarks (buy-and-hold, equal-weight)

**Constraints**:
- Transaction cost: 0.1% per trade
- Maximum positions: 5 concurrent
- Position size: 20% of capital each
- Stop loss: 5%
- Take profit: 10%

**Key Modules**:
- `src/backtesting/engine.py`
- `src/backtesting/portfolio.py`
- `src/backtesting/metrics.py`

### 5. Application Layer

**Technology**: Streamlit, FastAPI (optional)

**Responsibilities**:
- Display recommendations
- Visualize backtesting results
- Show model performance metrics
- Interactive charts

**Key Modules**:
- `src/dashboard/app.py` (Streamlit)
- `src/dashboard/components.py`

### 6. Deployment Layer

**Technology**: Google Cloud Platform

**Components**:
- **GCS**: Store processed data and models
- **Dataproc**: Run Spark jobs
- **Cloud Run**: Host Streamlit dashboard

**Key Files**:
- `deployment/gcp_setup.sh`
- `deployment/dataproc_job.py`
- `deployment/Dockerfile`

## Data Flow

1. **Ingestion**: Kaggle → Local/GCS (raw CSV)
2. **Processing**: PySpark → Clean, validate → Parquet
3. **Features**: Spark SQL → Technical indicators → Parquet
4. **Training**: Spark MLlib → Train model → Model artifact
5. **Prediction**: Model → Predictions → Parquet/CSV
6. **Backtesting**: Python → Simulate trades → Results JSON
7. **Visualization**: Streamlit → Dashboard → User

## Data Validation Strategy

### No Look-Ahead Bias
- Features use only past and current data
- Target uses future data (defined horizon)
- Chronological train/test split
- Held-out test stocks never seen during training

### Data Leakage Prevention
- Test stocks excluded from feature calculation statistics
- No overlapping prediction windows in evaluation
- Strict time-based validation

## Scalability Considerations

### Local Development
- Process subset of data for faster iteration
- Use `spark.master = "local[*]"`
- Sample data for exploratory analysis

### Production (GCP)
- Full dataset processing on Dataproc
- Distributed storage on GCS
- Auto-scaling Spark clusters
- Serverless options (Dataproc Serverless)

## Technology Justification

| Component | Technology | Reason |
|-----------|-----------|--------|
| Data Processing | PySpark | Handle large 1-min frequency data |
| Storage | Parquet | Columnar format, efficient compression |
| ML | Spark MLlib | Distributed training, integrates with Spark |
| HDFS | Hadoop | Demonstrate Hadoop requirement |
| Dashboard | Streamlit | Fast prototyping, Python-native |
| Cloud | GCP | Modern, cost-effective, Dataproc integration |

## Security Considerations

- Kaggle credentials: Environment variables only
- GCP credentials: Service accounts, never committed
- API keys: `.env` files (gitignored)
- Data: Anonymized where possible

## Future Enhancements (Not in Current Scope)

- Real-time data streaming (Kafka, Spark Streaming)
- Broker API integration (live trading)
- Deep learning models (TensorFlow on Spark)
- Advanced risk models
- Multi-asset portfolio optimization

## References

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Google Cloud Dataproc](https://cloud.google.com/dataproc/docs)
