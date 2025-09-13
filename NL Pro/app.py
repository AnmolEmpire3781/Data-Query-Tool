# app.py
import os
from dotenv import load_dotenv
load_dotenv()


import io
import json
from datetime import datetime
from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype
import pandas as pd
from flask import Flask, jsonify, render_template, request, send_file
from flask_cors import CORS

from services import db, gemini  # our helpers

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")
app.config["ASSET_VERSION"] = os.getenv("ASSET_VERSION", "6")  # bump to bust JS/CSS cache

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "")
if ALLOWED_ORIGINS:
    CORS(app, origins=[o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()])

# Storage for local history
HISTORY_PATH = os.path.join("storage", "query_history.json")
os.makedirs("storage", exist_ok=True)

# Load schema (and dialect) at startup
FULL_SCHEMA = db.get_schema()                 # {table: [ {name,type,pk,fk}, ... ], ...}
DIALECT = db.get_dialect()                    # e.g. 'postgresql'


# ------------------------------ helpers ------------------------------

def _subset_schema(full_schema: dict, tables: list[str] | None) -> dict:
    if not tables:
        return full_schema
    wanted = set(tables)
    return {t: cols for t, cols in full_schema.items() if t in wanted}

def _dtype_label(series: pd.Series) -> str:
    # 1) native datetime
    if is_datetime64_any_dtype(series):
        return "date"
    # 2) native numeric
    if is_numeric_dtype(series):
        return "number"
    # 3) object/string/etc.: try to coerce to numeric
    try:
        coerced = pd.to_numeric(series.dropna(), errors="coerce")
        # if we could parse (not all NaN), treat as numeric
        if not coerced.empty and coerced.notna().any():
            # optional: if *all* non-null coerced values are numeric, it’s numeric
            if coerced.notna().all():
                return "number"
    except Exception:
        pass
    return "text"

# app.py (replace your _rows_for_json with this)
from decimal import Decimal
import numpy as np

def _rows_for_json(df: pd.DataFrame):
    def conv(v):
        if v is None or (isinstance(v, float) and (pd.isna(v) or np.isnan(v))):
            return None
        if isinstance(v, Decimal):
            return float(v)
        # numpy scalars -> python scalars
        if isinstance(v, (np.integer, np.floating)):
            return float(v) if isinstance(v, np.floating) else int(v)
        return v
    out = []
    for row in df.itertuples(index=False, name=None):
        out.append([conv(v) for v in row])
    return out

def _append_history(entry: dict):
    try:
        items = []
        if os.path.exists(HISTORY_PATH):
            with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                items = json.load(f)
        items.insert(0, entry)
        items = items[:200]
        with open(HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2)
    except Exception:
        pass

def _read_history():
    try:
        if os.path.exists(HISTORY_PATH):
            with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return []


# ------------------------------ routes ------------------------------

@app.route("/", methods=["GET"])
def index():
    return render_template(
        "index.html",
        schema_json=FULL_SCHEMA,
        asset_version=app.config["ASSET_VERSION"]
    )

@app.route("/schema", methods=["GET"])
def schema():
    return jsonify({"ok": True, "schema": FULL_SCHEMA})

@app.route("/history", methods=["GET", "DELETE"])
def history():
    if request.method == "DELETE":
        try:
            if os.path.exists(HISTORY_PATH):
                os.remove(HISTORY_PATH)
        except Exception:
            pass
        return jsonify({"ok": True})
    return jsonify({"ok": True, "items": _read_history()})


@app.route("/query", methods=["POST"])
def query():
    """
    Body:
    {
      "question": "natural language question",   # optional if sql_override present
      "tables": ["sample_data", ...],            # optional
      "sql_override": "SELECT ...",              # optional: run raw SQL directly (SELECT-only)
    }
    """
    payload = request.get_json(force=True, silent=True) or {}
    question = (payload.get("question") or "").strip()
    tables = payload.get("tables") or []
    sql_override = (payload.get("sql_override") or "").strip()

    # Build schema subset for generation
    schema_subset = _subset_schema(FULL_SCHEMA, tables)

    # 1) Get SQL: if sql_override provided, use it; else generate with Gemini
    if sql_override:
        sql = sql_override
    else:
        if not question:
            return jsonify({"ok": False, "error": "Question is required."}), 400
        try:
            sql = gemini.generate_sql(question, schema_subset, dialect=DIALECT)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Failed to generate SQL: {e}"}), 500

    # 2) Execute SQL (DataFrame)
    try:
        df = db.run_sql(sql)
        for c in df.columns:
         if df[c].dtype == "object":
          df[c] = pd.to_numeric(df[c], errors="ignore")
       
        if not isinstance(df, pd.DataFrame):
            return jsonify({"ok": False, "error": "DB adapter did not return a DataFrame."}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": f"Database error: {e}", "sql": sql}), 400

    # 3) Column types for charting
    col_types = {c: _dtype_label(df[c]) for c in df.columns}

    # 4) Persist history
    _append_history({
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "question": question or "(raw SQL)",
        "sql": sql
    })

    return jsonify({
        "ok": True,
        "sql": sql,
        "columns": list(df.columns),
        "types": col_types,
        "rows": _rows_for_json(df),
    })


@app.route("/export/csv", methods=["POST"])
def export_csv():
    payload = request.get_json(force=True, silent=True) or {}
    sql = (payload.get("sql") or "").strip()
    if not sql:
        return jsonify({"ok": False, "error": "SQL is required."}), 400

    try:
        df = db.run_sql(sql)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Database error: {e}"}), 400

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        as_attachment=True,
        download_name="query_results.csv",
        mimetype="text/csv"
    )


@app.route("/export/excel", methods=["POST"])
def export_excel():
    payload = request.get_json(force=True, silent=True) or {}
    sql = (payload.get("sql") or "").strip()
    if not sql:
        return jsonify({"ok": False, "error": "SQL is required."}), 400

    try:
        df = db.run_sql(sql)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Database error: {e}"}), 400

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="results")
    out.seek(0)
    return send_file(
        out,
        as_attachment=True,
        download_name="query_results.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


if __name__ == "__main__":
    app.run(debug=True)
