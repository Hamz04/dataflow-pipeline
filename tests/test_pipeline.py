import pytest, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ["DATABASE_URL"] = "sqlite:///./test_pipeline.db"
from unittest.mock import patch
import pandas as pd

def teardown_module():
    if os.path.exists("test_pipeline.db"): os.remove("test_pipeline.db")

def test_transform_filters_zero_prices():
    import pipeline
    records = [
        {"symbol": "bitcoin", "price": 50000, "volume": 1e9, "market_cap": 1e12},
        {"symbol": "bad", "price": 0, "volume": 0, "market_cap": 0},
    ]
    df = pipeline.transform(records)
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "bitcoin"

def test_transform_empty():
    import pipeline
    assert pipeline.transform([]).empty

def test_load_empty():
    import pipeline
    assert pipeline.load(pd.DataFrame()) == 0

def test_run_pipeline_with_mock():
    import pipeline
    mock_records = [{"symbol": "bitcoin", "price": 65000, "volume": 2e9, "market_cap": 1.2e12}]
    with patch.object(pipeline, "extract", return_value=mock_records):
        result = pipeline.run_pipeline()
    assert result["status"] == "success"
    assert result["records"] == 1
