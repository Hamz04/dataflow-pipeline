# DataFlow ETL Pipeline

![Python](https://img.shields.io/badge/python-3.12-blue)
![Streamlit](https://img.shields.io/badge/streamlit-1.35-red)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![License](https://img.shields.io/badge/license-MIT-green)

> Production ETL pipeline: fetches live crypto prices, transforms data, loads to SQLite/PostgreSQL, and serves a real-time Streamlit dashboard.

## Features
- Extract live prices from CoinGecko (free, no key needed)
- Transform + validate with pandas
- Load into SQLite (dev) or PostgreSQL (prod via DATABASE_URL env var)
- Real-time Streamlit dashboard with charts and run history
- Full pytest test suite (4 tests)
- Docker-ready, CI/CD via GitHub Actions

## Quick Start
```bash
git clone https://github.com/Hamz04/dataflow-pipeline
cd dataflow-pipeline
pip install -r requirements.txt

# Launch dashboard
streamlit run pipeline.py

# Run pipeline headless
python pipeline.py --pipeline
```

## Architecture
```
Extract  ->  CoinGecko API (bitcoin, ethereum, solana, cardano, chainlink)
Transform -> pandas (filter zero prices, add timestamp)
Load     ->  SQLite / PostgreSQL
Serve    ->  Streamlit dashboard (prices table + line chart + run history)
```

## Docker
```bash
docker build -t dataflow-pipeline .
docker run -p 8501:8501 dataflow-pipeline
# Open http://localhost:8501
```

## Testing
```bash
pytest tests/ -v
```

## License
MIT
