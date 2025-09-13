# services/db.py
import os
import re
from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/mydb"
)

_engine: Engine | None = None

def _engine_once() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL, future=True)
    return _engine

def get_dialect() -> str:
    return _engine_once().dialect.name  # 'postgresql', 'sqlite', etc.

def run_sql(sql: str) -> pd.DataFrame:
    """
    Execute SELECT-only SQL and return a pandas DataFrame.
    """
    if not re.match(r"(?is)^\s*select\b", sql or ""):
        raise ValueError("Only SELECT statements are allowed.")
    eng = _engine_once()
    with eng.connect() as conn:
        res = conn.execute(text(sql))
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
    return df

def get_schema() -> Dict[str, List[Dict]]:
    """
    Build schema description for LLM prompt/UI:
      { table_name: [ {name, type, pk, fk}, ... ], ... }
    """
    eng = _engine_once()
    insp = inspect(eng)
    schema: Dict[str, List[Dict]] = {}

    for table in insp.get_table_names():
        cols: List[Dict] = []
        pk_cols = set(insp.get_pk_constraint(table).get("constrained_columns") or [])
        fk_map = {}
        for fk in insp.get_foreign_keys(table):
            cols_fk = fk.get("constrained_columns", [])
            ref_tbl = fk.get("referred_table")
            ref_cols = fk.get("referred_columns", [])
            for c, rc in zip(cols_fk, ref_cols):
                fk_map[c] = f"{ref_tbl}.{rc}"
        for c in insp.get_columns(table):
            cols.append({
                "name": c["name"],
                "type": str(c.get("type")),
                "pk": c["name"] in pk_cols,
                "fk": fk_map.get(c["name"], "")
            })
        schema[table] = cols

    return schema
