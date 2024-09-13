"""DataFlow Pipeline - ETL with Streamlit dashboard."""
import os, logging
from datetime import datetime
from typing import Any
import httpx
import pandas as pd
import streamlit as st
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./pipeline.db")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

class Base(DeclarativeBase): pass

class StockPrice(Base):
    __tablename__ = "stock_prices"
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    price = Column(Float)
    volume = Column(Float)
    market_cap = Column(Float, nullable=True)
    fetched_at = Column(DateTime, default=datetime.utcnow)

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    id = Column(Integer, primary_key=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    records_processed = Column(Integer, default=0)
    status = Column(String, default="running")
    error = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)
SYMBOLS = ["bitcoin", "ethereum", "solana", "cardano", "chainlink"]

def extract(symbols: list[str]) -> list[dict]:
    ids = ",".join(symbols)
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true"
    try:
        with httpx.Client(timeout=10) as c:
            r = c.get(url); r.raise_for_status(); data = r.json()
        records = [{"symbol": s, "price": data[s].get("usd",0), "volume": data[s].get("usd_24h_vol",0), "market_cap": data[s].get("usd_market_cap",0)} for s in symbols if s in data]
        log.info(f"Extracted {len(records)} records"); return records
    except Exception as e:
        log.error(f"Extract failed: {e}"); return []

def transform(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    if df.empty: return df
    df = df[df["price"] > 0].copy()
    df["fetched_at"] = datetime.utcnow()
    log.info(f"Transformed {len(df)} records"); return df

def load(df: pd.DataFrame) -> int:
    if df.empty: return 0
    db = SessionLocal()
    try:
        rows = df[["symbol","price","volume","market_cap","fetched_at"]].to_dict("records")
        for row in rows: db.add(StockPrice(**row))
        db.commit(); log.info(f"Loaded {len(rows)} records"); return len(rows)
    except Exception as e:
        db.rollback(); log.error(f"Load failed: {e}"); return 0
    finally: db.close()

def run_pipeline() -> dict[str, Any]:
    db = SessionLocal(); run = PipelineRun(); db.add(run); db.commit(); db.refresh(run); run_id = run.id; db.close()
    try:
        raw = extract(SYMBOLS); df = transform(raw); count = load(df)
        db = SessionLocal(); run = db.query(PipelineRun).filter(PipelineRun.id == run_id).first()
        run.finished_at = datetime.utcnow(); run.records_processed = count; run.status = "success"; db.commit(); db.close()
        return {"status": "success", "records": count}
    except Exception as e:
        db = SessionLocal(); run = db.query(PipelineRun).filter(PipelineRun.id == run_id).first()
        run.finished_at = datetime.utcnow(); run.status = "failed"; run.error = str(e); db.commit(); db.close()
        return {"status": "failed", "error": str(e)}

def run_dashboard():
    st.set_page_config(page_title="DataFlow Pipeline", layout="wide")
    st.title("DataFlow ETL Pipeline Dashboard")
    if st.button("Run Pipeline Now"):
        with st.spinner("Running ETL..."):
            result = run_pipeline()
        st.success(f"Done! {result['records']} records loaded.") if result["status"] == "success" else st.error(f"Failed: {result.get('error')}")
    prices_df = pd.read_sql("SELECT symbol, price, volume, market_cap, fetched_at FROM stock_prices ORDER BY fetched_at DESC LIMIT 200", engine)
    runs_df = pd.read_sql("SELECT started_at, finished_at, records_processed, status FROM pipeline_runs ORDER BY started_at DESC LIMIT 20", engine)
    st.subheader("Latest Prices")
    if not prices_df.empty:
        latest = prices_df.groupby("symbol").first().reset_index()
        st.dataframe(latest[["symbol","price","market_cap"]], use_container_width=True)
        st.line_chart(prices_df.pivot_table(index="fetched_at", columns="symbol", values="price"))
    else:
        st.info("No data yet. Click 'Run Pipeline Now'.")
    st.subheader("Pipeline Run History")
    if not runs_df.empty: st.dataframe(runs_df, use_container_width=True)

if __name__ == "__main__":
    import sys
    if "--pipeline" in sys.argv: print(run_pipeline())
    else: run_dashboard()
