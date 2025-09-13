# # services/gemini.py
# """
# Gemini-powered SQL generator with:
# - rules for GROUP BY + SUM(COALESCE), case-insensitive matching
# - non-blank dimension filters
# - time-bucket rewrite (monthly/weekly/daily/quarterly/yearly) → period,value
# - keyword-glue sanitizer to fix tiny spacing errors
# """

# from __future__ import annotations

# import os
# import re
# from typing import Dict, List

# import google.generativeai as genai

# GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
# if not GEMINI_API_KEY:
#     raise RuntimeError("GEMINI_API_KEY is not configured.")
# genai.configure(api_key=GEMINI_API_KEY)

# GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

# _model = genai.GenerativeModel(
#     model_name=GEMINI_MODEL,
#     generation_config={
#         "temperature": 0.15,
#         "top_p": 0.9,
#         "top_k": 32,
#         "max_output_tokens": 512,
#     },
# )

# # -------------------- prompt helpers --------------------

# def _format_schema_prompt(schema_metadata: Dict[str, List[Dict]]) -> str:
#     lines = []
#     for table, cols in schema_metadata.items():
#         lines.append(table)
#         for c in cols:
#             t = c.get("type", "TEXT")
#             s = f"  - {c['name']} : {t}"
#             if c.get("pk"):
#                 s += " (PK)"
#             if c.get("fk"):
#                 s += f" (FK->{c['fk']})"
#             lines.append(s)
#         lines.append("")
#     return "\n".join(lines).strip()

# def _build_rules_text(dialect: str) -> str:
#     if dialect.lower().startswith("postgres"):
#         case_rule = "- Use ILIKE for case-insensitive string comparisons."
#     else:
#         case_rule = "- Use LOWER(column) = LOWER('literal') for case-insensitive comparisons."

#     bucket_rule = (
#         "7) If the question mentions daily/weekly/monthly/quarterly/yearly, return TWO columns:\n"
#         "   period (time bucket) and value (the aggregated measure).\n"
#         "   In PostgreSQL, use DATE_TRUNC('bucket', \"date_col\"), GROUP BY 1, ORDER BY 1 ASC.\n"
#     )

#     return f"""
# You are a careful SQL generator. Return exactly ONE SQL SELECT statement and nothing else.

# RULES
# 1) Use only the tables/columns shown in the schema. Quote identifiers with double-quotes.
# 2) SELECT only. No DDL/DML, no comments, no markdown.
# 3) {case_rule}
# 4) For superlatives like "highest/lowest/top/bottom" by a dimension:
#    - Aggregate numeric measures with SUM(COALESCE(col,0)) or COUNT(*).
#    - GROUP BY the dimension column(s).
#    - Filter out NULL/blank dimension values using:
#      column IS NOT NULL AND LENGTH(TRIM(column)) > 0
#    - ORDER BY the aggregated value (DESC for highest) and add LIMIT as needed.
# 5) Use ISO 'YYYY-MM-DD' for dates; do not quote pure numbers.
# 6) Prefer LIMIT over vendor-specific TOP/OFFSET forms.
# 7) Always alias aggregate expressions with informative, distinct names.
#   Examples: AVG(age) AS avg_age, SUM(actual_spends) AS total_spends.
# {bucket_rule}
# """.strip()

# def _build_prompt(question: str, schema_metadata: Dict[str, List[Dict]], dialect: str) -> str:
#     return f"""
# {_build_rules_text(dialect)}

# SCHEMA
# {_format_schema_prompt(schema_metadata)}

# QUESTION
# {question}

# Return a single SQL SELECT statement using the schema above. No commentary.
# """.strip()

# # -------------------- sanitizers & transforms --------------------

# _MAJOR = ["SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"]

# def _strip_code_fences(text: str) -> str:
#     text = re.sub(r"(?is)```sql(.*?)```", r"\1", text)
#     text = re.sub(r"(?is)```(.*?)```", r"\1", text)
#     return text.strip()

# def _fix_keyword_glue(sql: str) -> str:
#     sql = re.sub(r'>\s*0\s*(GROUP\b)', r'> 0 \1', sql, flags=re.IGNORECASE)
#     sql = re.sub(r'\)\s*(GROUP\b)', r') \1', sql, flags=re.IGNORECASE)
#     sql = re.sub(r'(\d)\s*(GROUP\b)', r'\1 \2', sql, flags=re.IGNORECASE)
#     for kw in _MAJOR:
#         pattern = r'(?i)(\S)(' + re.escape(kw) + r')'
#         sql = re.sub(pattern, r'\1 \2', sql)
#     sql = re.sub(r'[ \t]+', ' ', sql)
#     sql = re.sub(r'\s+\n', '\n', sql)
#     sql = re.sub(r'\n\s+', '\n', sql)
#     return sql.strip()

# def _clean_sql(raw: str) -> str:
#     text = _strip_code_fences(raw)
#     m = re.search(r'(?is)\bselect\b', text)
#     if m:
#         text = text[m.start():]
#     lines = [ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("--")]
#     text = "\n".join(lines).strip()
#     text = text.rstrip("; \n\t")
#     return _fix_keyword_glue(text)

# def _wrap_sum_with_coalesce(sql: str) -> str:
#     def repl(m):
#         inner = m.group(1)
#         if re.search(r'(?i)\bcoalesce\s*\(', inner):
#             return m.group(0)
#         return f"SUM(COALESCE({inner}, 0))"
#     return re.sub(r'(?is)\bsum\s*\(\s*([^\)]+?)\s*\)', repl, sql)

# def _columns_in_group_by(sql: str) -> List[str]:
#     m = re.search(r'(?is)\bgroup\s+by\b(.*?)(?:\border\s+by\b|\blimit\b|$)', sql)
#     if not m:
#         return []
#     tokens = [t.strip() for t in m.group(1).split(",") if t.strip()]
#     cols = []
#     for t in tokens:
#         if re.fullmatch(r'\d+', t):
#             continue
#         mm = re.search(r'\"?([A-Za-z_][A-Za-z0-9_]*)\"?$', t)
#         if mm:
#             cols.append(mm.group(1))
#     return cols

# def _inject_non_blank_filter(sql: str, text_columns: List[str], dialect: str) -> str:
#     gb_cols = _columns_in_group_by(sql)
#     if not gb_cols:
#         return sql
#     dims = [c for c in gb_cols if c in set(text_columns)]
#     if not dims:
#         return sql

#     predicate = " AND ".join(
#         [f"\"{c}\" IS NOT NULL AND LENGTH(TRIM(\"{c}\")) > 0" for c in dims]
#     )
#     if re.search(r'(?is)\bwhere\b', sql):
#         return re.sub(r'(?is)\bwhere\b', f"WHERE {predicate} AND ", sql, count=1)
#     # insert WHERE after FROM … before GROUP/ORDER/LIMIT
#     m = re.search(r'(?is)\bfrom\b\s+.+?(?=\bwhere\b|\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)', sql)
#     if m:
#         insert_at = m.end()
#         return sql[:insert_at] + f" WHERE {predicate} " + sql[insert_at:]
#     return sql + f" WHERE {predicate} "

# def _apply_time_bucket_if_needed(sql: str,
#                                  question: str,
#                                  schema_metadata: Dict[str, List[Dict]],
#                                  dialect: str) -> str:
#     q = question.lower()
#     bucket = None
#     if re.search(r'\b(monthly|per month|by month|each month)\b', q): bucket = "month"
#     elif re.search(r'\b(daily|per day|by day|each day)\b', q):       bucket = "day"
#     elif re.search(r'\b(weekly|per week|by week|each week)\b', q):   bucket = "week"
#     elif re.search(r'\b(quarterly|per quarter|by quarter)\b', q):    bucket = "quarter"
#     elif re.search(r'\b(yearly|per year|by year|annual|annually)\b', q): bucket = "year"

#     if not bucket or re.search(r'(?i)\bgroup\s+by\b', sql):
#         return sql

#     # find a date-like column
#     date_cols: List[str] = []
#     for _, cols in schema_metadata.items():
#         for c in cols:
#             ctype = str(c.get("type", "")).lower()
#             name = c["name"]
#             if ("date" in ctype) or ("timestamp" in ctype) or re.search(r'(date|month|day|year)$', name, re.IGNORECASE):
#                 date_cols.append(name)
#     if not date_cols:
#         return sql

#     date_col = next((n for n in date_cols if n.lower() == "month"), date_cols[0])
#     qcol = f"\"{date_col}\"" if not date_col.startswith('"') else date_col

#     if dialect.lower().startswith("postgres"):
#         mapping = {
#             "day":     "DATE_TRUNC('day', {c})::date",
#             "week":    "DATE_TRUNC('week', {c})::date",
#             "month":   "DATE_TRUNC('month', {c})::date",
#             "quarter": "DATE_TRUNC('quarter', {c})::date",
#             "year":    "DATE_TRUNC('year', {c})::date",
#         }
#         bucket_expr = mapping[bucket].format(c=qcol)
#     elif dialect.lower().startswith("sqlite"):
#         fmt = {"day": "%Y-%m-%d", "week": "%Y-%W", "month": "%Y-%m", "year": "%Y"}
#         bucket_expr = f"strftime('{fmt.get(bucket, '%Y-%m')}', {qcol})"
#     else:
#         bucket_expr = qcol

#     # Extract SUM(...) in SELECT; keep rest after FROM
#     m = re.search(r'(?is)select\s+(.*?)\s+from\b', sql)
#     sum_expr = "SUM(1)"
#     rest = sql
#     if m:
#         select_part = m.group(1)
#         sm = re.search(r'(?is)(sum\s*\([^\)]*\))', select_part)
#         if sm: sum_expr = sm.group(1)
#         rest = sql[m.end():]

#     new_sql = f"SELECT {bucket_expr} AS period, {sum_expr} AS value FROM {rest}"

#     if not re.search(r'(?i)\bgroup\s+by\b', new_sql):
#         m_order = re.search(r'(?i)\border\s+by\b', new_sql)
#         m_limit = re.search(r'(?i)\blimit\b', new_sql)
#         cut = min([p for p in [m_order.start() if m_order else None,
#                                m_limit.start() if m_limit else None]
#                    if p is not None] or [len(new_sql)])
#         new_sql = new_sql[:cut] + " GROUP BY 1 " + new_sql[cut:]
#     if not re.search(r'(?i)\border\s+by\b', new_sql):
#         new_sql += " ORDER BY 1"

#     return new_sql


# # -------------------- public API --------------------

# def generate_sql(natural_language_query: str,
#                  schema_metadata: Dict[str, List[Dict]],
#                  dialect: str = "postgresql") -> str:
#     prompt = _build_prompt(natural_language_query, schema_metadata, dialect)

#     try:
#         resp = _model.generate_content(prompt)
#         raw = resp.text or ""
#     except Exception as e:
#         raise RuntimeError(f"Gemini API error: {e}")

#     sql = _clean_sql(raw)
#     sql = _wrap_sum_with_coalesce(sql)

#     # collect text-like columns for dimension filter
#     text_cols: List[str] = []
#     for _, cols in schema_metadata.items():
#         for c in cols:
#             ctype = str(c.get("type", "")).lower()
#             if "text" in ctype or "char" in ctype or "string" in ctype:
#                 text_cols.append(c["name"])

#     if re.search(r"(?i)\bgroup\s+by\b", sql) or re.search(r"(?i)\bsum\s*\(", sql):
#         sql = _inject_non_blank_filter(sql, text_cols, dialect)

#     # rewrite to time buckets if the question asks for per-period results
#     sql = _apply_time_bucket_if_needed(sql, natural_language_query, schema_metadata, dialect)

#     sql = _clean_sql(sql)  # final spacing/cleanup
#     return sql

#changedddd

# # services/gemini.py
# """
# Gemini-powered SQL generator with:
# - rules for GROUP BY + SUM(COALESCE), case-insensitive matching
# - non-blank dimension filters
# - time-bucket rewrite (daily/weekly/monthly/quarterly/yearly) → no timestamps
# - keyword-glue sanitizer to fix tiny spacing errors
# - guidance to scale large aggregates (millions/billions) with _m / _b suffixes
# """

# from __future__ import annotations

# import os
# import re
# from typing import Dict, List

# import google.generativeai as genai

# # -------------------- model bootstrap --------------------

# GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
# if not GEMINI_API_KEY:
#     raise RuntimeError("GEMINI_API_KEY is not configured.")
# genai.configure(api_key=GEMINI_API_KEY)

# GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

# _model = genai.GenerativeModel(
#     model_name=GEMINI_MODEL,
#     generation_config={
#         "temperature": 0.15,
#         "top_p": 0.9,
#         "top_k": 32,
#         "max_output_tokens": 512,
#     },
# )

# # -------------------- prompt helpers --------------------

# def _format_schema_prompt(schema_metadata: Dict[str, List[Dict]]) -> str:
#     lines = []
#     for table, cols in schema_metadata.items():
#         lines.append(table)
#         for c in cols:
#             t = c.get("type", "TEXT")
#             s = f"  - {c['name']} : {t}"
#             if c.get("pk"):
#                 s += " (PK)"
#             if c.get("fk"):
#                 s += f" (FK->{c['fk']})"
#             lines.append(s)
#         lines.append("")
#     return "\n".join(lines).strip()


# def _build_rules_text(dialect: str) -> str:
#     if dialect.lower().startswith("postgres"):
#         case_rule = "- Use ILIKE for case-insensitive string comparisons."
#     else:
#         case_rule = "- Use LOWER(column) = LOWER('literal') for case-insensitive comparisons."

#     # Strong, explicit guidance for time buckets and scaling
#     bucket_scaling_rules = f"""
# DATE/TIME BUCKETS (NO TIMESTAMPS IN OUTPUT)
# - When the question mentions daily/weekly/monthly/quarterly/yearly, return bucketed results.
# - DO NOT return timestamps in result columns. Cast/format to DATE or TEXT.
# - PostgreSQL (examples, replace <ts> by the actual timestamp/date column):
#   * day:      (DATE_TRUNC('day', <ts>))::date                                    AS day
#   * week:     TO_CHAR(DATE_TRUNC('week', <ts>), 'IYYY-IW')                       AS week
#   * month:    (DATE_TRUNC('month', <ts>))::date                                   AS month
#   * quarter:  (EXTRACT(YEAR FROM <ts>) || '-Q' || EXTRACT(QUARTER FROM <ts>))     AS quarter
#   * year:     EXTRACT(YEAR FROM <ts>)::int                                        AS year
# - Always GROUP BY the first bucket column and ORDER BY it ascending.

# SCALING LARGE AGGREGATES (READABILITY)
# - If aggregated values (e.g., spends/revenue/counts) will be very large, scale them for readability:
#   * millions:  ROUND(SUM(COALESCE(amount, 0)) / 1e6, 2)  AS total_amount_m
#   * billions:  ROUND(SUM(COALESCE(amount, 0)) / 1e9, 2)  AS total_amount_b
# - Pick a sensible scale (M/B) when the user doesn’t specify. Add the _m/_b suffix so units are explicit.
# """

#     return f"""
# You are a careful SQL generator. Return exactly ONE SQL SELECT statement and nothing else.

# RULES
# 1) Use only the tables/columns shown in the schema. Quote identifiers with double-quotes.
# 2) SELECT only. No DDL/DML, no comments, no markdown.
# 3) {case_rule}
# 4) For superlatives like "highest/lowest/top/bottom" by a dimension:
#    - Aggregate numeric measures with SUM(COALESCE(col,0)) or COUNT(*).
#    - GROUP BY the dimension column(s).
#    - Filter out NULL/blank dimension values using:
#      column IS NOT NULL AND LENGTH(TRIM(column)) > 0
#    - ORDER BY the aggregated value (DESC for highest) and add LIMIT as needed.
# 5) Use ISO 'YYYY-MM-DD' for dates; do not quote pure numbers.
# 6) Prefer LIMIT over vendor-specific TOP/OFFSET forms.
# 7) Always alias aggregate expressions with informative, distinct names (e.g., AVG(age) AS avg_age).
# {bucket_scaling_rules}
# """.strip()


# def _build_prompt(question: str, schema_metadata: Dict[str, List[Dict]], dialect: str) -> str:
#     return f"""
# {_build_rules_text(dialect)}

# SCHEMA
# {_format_schema_prompt(schema_metadata)}

# QUESTION
# {question}

# Return a single SQL SELECT statement using the schema above. No commentary.
# """.strip()

# # -------------------- sanitizers & transforms --------------------

# _MAJOR = ["SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"]

# def _strip_code_fences(text: str) -> str:
#     text = re.sub(r"(?is)```sql(.*?)```", r"\1", text)
#     text = re.sub(r"(?is)```(.*?)```", r"\1", text)
#     return text.strip()

# def _fix_keyword_glue(sql: str) -> str:
#     # ensure "… > 0 GROUP" etc. have spacing
#     sql = re.sub(r'>\s*0\s*(GROUP\b)', r'> 0 \1', sql, flags=re.IGNORECASE)
#     sql = re.sub(r'\)\s*(GROUP\b)', r') \1', sql, flags=re.IGNORECASE)
#     sql = re.sub(r'(\d)\s*(GROUP\b)', r'\1 \2', sql, flags=re.IGNORECASE)
#     for kw in _MAJOR:
#         pattern = r'(?i)(\S)(' + re.escape(kw) + r')'
#         sql = re.sub(pattern, r'\1 \2', sql)
#     sql = re.sub(r'[ \t]+', ' ', sql)
#     sql = re.sub(r'\s+\n', '\n', sql)
#     sql = re.sub(r'\n\s+', '\n', sql)
#     return sql.strip()

# def _clean_sql(raw: str) -> str:
#     text = _strip_code_fences(raw)
#     m = re.search(r'(?is)\bselect\b', text)
#     if m:
#         text = text[m.start():]
#     lines = [ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("--")]
#     text = "\n".join(lines).strip()
#     text = text.rstrip("; \n\t")
#     return _fix_keyword_glue(text)

# def _wrap_sum_with_coalesce(sql: str) -> str:
#     """Ensure SUM(col) -> SUM(COALESCE(col,0)) if not already coalesced."""
#     def repl(m):
#         inner = m.group(1)
#         if re.search(r'(?i)\bcoalesce\s*\(', inner):
#             return m.group(0)
#         return f"SUM(COALESCE({inner}, 0))"
#     return re.sub(r'(?is)\bsum\s*\(\s*([^\)]+?)\s*\)', repl, sql)

# def _columns_in_group_by(sql: str) -> List[str]:
#     m = re.search(r'(?is)\bgroup\s+by\b(.*?)(?:\border\s+by\b|\blimit\b|$)', sql)
#     if not m:
#         return []
#     tokens = [t.strip() for t in m.group(1).split(",") if t.strip()]
#     cols = []
#     for t in tokens:
#         if re.fullmatch(r'\d+', t):
#             continue
#         mm = re.search(r'\"?([A-Za-z_][A-Za-z0-9_]*)\"?$', t)
#         if mm:
#             cols.append(mm.group(1))
#     return cols

# def _inject_non_blank_filter(sql: str, text_columns: List[str], dialect: str) -> str:
#     gb_cols = _columns_in_group_by(sql)
#     if not gb_cols:
#         return sql
#     dims = [c for c in gb_cols if c in set(text_columns)]
#     if not dims:
#         return sql

#     predicate = " AND ".join(
#         [f"\"{c}\" IS NOT NULL AND LENGTH(TRIM(\"{c}\")) > 0" for c in dims]
#     )
#     if re.search(r'(?is)\bwhere\b', sql):
#         return re.sub(r'(?is)\bwhere\b', f"WHERE {predicate} AND ", sql, count=1)
#     # insert WHERE after FROM … before GROUP/ORDER/LIMIT
#     m = re.search(r'(?is)\bfrom\b\s+.+?(?=\bwhere\b|\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)', sql)
#     if m:
#         insert_at = m.end()
#         return sql[:insert_at] + f" WHERE {predicate} " + sql[insert_at:]
#     return sql + f" WHERE {predicate} "

# def _apply_time_bucket_if_needed(sql: str,
#                                  question: str,
#                                  schema_metadata: Dict[str, List[Dict]],
#                                  dialect: str) -> str:
#     """
#     If the NL question clearly asks for per-period results but the generated SQL
#     isn't bucketed already, rewrite it into a 2-column (period,value) query.

#     Ensures **no timestamps** are returned in the 'period' column.
#     """
#     q = question.lower()
#     bucket = None
#     if re.search(r'\b(monthly|per month|by month|each month)\b', q): bucket = "month"
#     elif re.search(r'\b(daily|per day|by day|each day)\b', q):       bucket = "day"
#     elif re.search(r'\b(weekly|per week|by week|each week)\b', q):   bucket = "week"
#     elif re.search(r'\b(quarterly|per quarter|by quarter)\b', q):    bucket = "quarter"
#     elif re.search(r'\b(yearly|per year|by year|annual|annually)\b', q): bucket = "year"

#     # If it's already grouped, assume the LLM handled it.
#     if not bucket or re.search(r'(?i)\bgroup\s+by\b', sql):
#         return sql

#     # Choose a date-like column
#     date_cols: List[str] = []
#     for _, cols in schema_metadata.items():
#         for c in cols:
#             ctype = str(c.get("type", "")).lower()
#             name = c["name"]
#             if ("date" in ctype) or ("timestamp" in ctype) or re.search(r'(date|month|day|year)$', name, re.IGNORECASE):
#                 date_cols.append(name)
#     if not date_cols:
#         return sql

#     # Prefer a column literally named 'month' if present, else the first date-ish col
#     date_col = next((n for n in date_cols if n.lower() == "month"), date_cols[0])
#     qcol = f"\"{date_col}\"" if not date_col.startswith('"') else date_col

#     # Produce timestamp-free bucket expressions
#     if dialect.lower().startswith("postgres"):
#         mapping = {
#             "day":     f"(DATE_TRUNC('day', {qcol}))::date",
#             "week":    f"TO_CHAR(DATE_TRUNC('week', {qcol}), 'IYYY-IW')",
#             "month":   f"(DATE_TRUNC('month', {qcol}))::date",
#             "quarter": f"(EXTRACT(YEAR FROM {qcol}) || '-Q' || EXTRACT(QUARTER FROM {qcol}))",
#             "year":    f"EXTRACT(YEAR FROM {qcol})::int",
#         }
#         bucket_expr = mapping[bucket]
#     elif dialect.lower().startswith("sqlite"):
#         fmt = {"day": "%Y-%m-%d", "week": "%Y-%W", "month": "%Y-%m", "year": "%Y"}
#         if bucket == "quarter":
#             # best-effort textual quarter for SQLite (not perfect but non-timestamp)
#             bucket_expr = f"(strftime('%Y', {qcol}) || '-Q' || ((cast(strftime('%m', {qcol}) as int)+2)/3))"
#         else:
#             bucket_expr = f"strftime('{fmt.get(bucket, '%Y-%m')}', {qcol})"
#     else:
#         # Fallback: return the raw column (could be DATE already). Still no timestamptz casting here.
#         bucket_expr = qcol

#     # Try to reuse first SUM(...) we see; otherwise count rows
#     m = re.search(r'(?is)select\s+(.*?)\s+from\b', sql)
#     sum_expr = "COUNT(*)"
#     rest = sql
#     if m:
#         select_part = m.group(1)
#         sm = re.search(r'(?is)(sum\s*\([^\)]*\))', select_part)
#         if sm:
#             sum_expr = sm.group(1)
#         rest = sql[m.end():]

#     new_sql = f"SELECT {bucket_expr} AS period, {sum_expr} AS value FROM {rest}"

#     # Add GROUP BY period if missing
#     if not re.search(r'(?i)\bgroup\s+by\b', new_sql):
#         m_order = re.search(r'(?i)\border\s+by\b', new_sql)
#         m_limit = re.search(r'(?i)\blimit\b', new_sql)
#         cut = min([p for p in [m_order.start() if m_order else None,
#                                m_limit.start() if m_limit else None]
#                    if p is not None] or [len(new_sql)])
#         new_sql = new_sql[:cut] + " GROUP BY 1 " + new_sql[cut:]
#     if not re.search(r'(?i)\border\s+by\b', new_sql):
#         new_sql += " ORDER BY 1"
#     return new_sql

# # -------------------- public API --------------------

# def generate_sql(natural_language_query: str,
#                  schema_metadata: Dict[str, List[Dict]],
#                  dialect: str = "postgresql") -> str:
#     """Generate SQL with Gemini, then post-process for robustness."""
#     prompt = _build_prompt(natural_language_query, schema_metadata, dialect)

#     try:
#         resp = _model.generate_content(prompt)
#         raw = resp.text or ""
#     except Exception as e:
#         raise RuntimeError(f"Gemini API error: {e}")

#     sql = _clean_sql(raw)
#     sql = _wrap_sum_with_coalesce(sql)

#     # collect text-like columns for dimension filter
#     text_cols: List[str] = []
#     for _, cols in schema_metadata.items():
#         for c in cols:
#             ctype = str(c.get("type", "")).lower()
#             if "text" in ctype or "char" in ctype or "string" in ctype:
#                 text_cols.append(c["name"])

#     if re.search(r"(?i)\bgroup\s+by\b", sql) or re.search(r"(?i)\bsum\s*\(", sql):
#         sql = _inject_non_blank_filter(sql, text_cols, dialect)

#     # rewrite to time buckets (no timestamps) if the question asks for per-period results
#     sql = _apply_time_bucket_if_needed(sql, natural_language_query, schema_metadata, dialect)

#     sql = _clean_sql(sql)  # final whitespace/keyword spacing
#     return sql
# services/gemini.py
"""
Gemini-powered SQL generator with:
- rules for GROUP BY + SUM(COALESCE), case-insensitive matching
- non-blank dimension filters
- time-bucket rewrite (monthly/weekly/daily/quarterly/yearly) → period,value
  (period is an ISO date string 'YYYY-MM-DD', never a timestamp)
- keyword-glue sanitizer to fix tiny spacing errors
"""

from __future__ import annotations

import os
import re
from typing import Dict, List

import google.generativeai as genai

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY is not configured.")
genai.configure(api_key=GEMINI_API_KEY)

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

_model = genai.GenerativeModel(
    model_name=GEMINI_MODEL,
    generation_config={
        "temperature": 0.15,
        "top_p": 0.9,
        "top_k": 32,
        "max_output_tokens": 512,
    },
)

# -------------------- prompt helpers --------------------

def _format_schema_prompt(schema_metadata: Dict[str, List[Dict]]) -> str:
    lines = []
    for table, cols in schema_metadata.items():
        lines.append(table)
        for c in cols:
            t = c.get("type", "TEXT")
            s = f"  - {c['name']} : {t}"
            if c.get("pk"):
                s += " (PK)"
            if c.get("fk"):
                s += f" (FK->{c['fk']})"
            lines.append(s)
        lines.append("")
    return "\n".join(lines).strip()

def _build_rules_text(dialect: str) -> str:
    if dialect.lower().startswith("postgres"):
        case_rule = "- Use ILIKE for case-insensitive string comparisons."
    else:
        case_rule = "- Use LOWER(column) = LOWER('literal') for case-insensitive comparisons."

    # NOTE: explicitly demand ::date + TO_CHAR to avoid timestamps/time zones in results
    bucket_rule = (
        "7) If the question mentions daily/weekly/monthly/quarterly/yearly, return TWO columns:\n"
        "   period (time bucket as ISO string 'YYYY-MM-DD') and value (the aggregated measure).\n"
        "   In PostgreSQL, use TO_CHAR(DATE_TRUNC('bucket', \"date_col\")::date, 'YYYY-MM-DD') AS period,\n"
        "   then GROUP BY 1 and ORDER BY 1 ASC.\n"
    )

    return f"""
You are a careful SQL generator. Return exactly ONE SQL SELECT statement and nothing else.

RULES
1) Use only the tables/columns shown in the schema. Quote identifiers with double-quotes.
2) SELECT only. No DDL/DML, no comments, no markdown.
3) {case_rule}
4) For superlatives like "highest/lowest/top/bottom" by a dimension:
   - Aggregate numeric measures with SUM(COALESCE(col,0)) or COUNT(*).
   - GROUP BY the dimension column(s).
   - Filter out NULL/blank dimension values using:
     column IS NOT NULL AND LENGTH(TRIM(column)) > 0
   - ORDER BY the aggregated value (DESC for highest) and add LIMIT as needed.
5) Use ISO 'YYYY-MM-DD' for dates; do not quote pure numbers.
6) Prefer LIMIT over vendor-specific TOP/OFFSET forms.
7) Always alias aggregate expressions with informative, distinct names.
   Examples: AVG(age) AS avg_age, SUM(actual_spends) AS total_spends.
{bucket_rule}
""".strip()

def _build_prompt(question: str, schema_metadata: Dict[str, List[Dict]], dialect: str) -> str:
    return f"""
{_build_rules_text(dialect)}

SCHEMA
{_format_schema_prompt(schema_metadata)}

QUESTION
{question}

Return a single SQL SELECT statement using the schema above. No commentary.
""".strip()

# -------------------- sanitizers & transforms --------------------

_MAJOR = ["SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"]

def _strip_code_fences(text: str) -> str:
    text = re.sub(r"(?is)```sql(.*?)```", r"\1", text)
    text = re.sub(r"(?is)```(.*?)```", r"\1", text)
    return text.strip()

def _fix_keyword_glue(sql: str) -> str:
    sql = re.sub(r'>\s*0\s*(GROUP\b)', r'> 0 \1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\)\s*(GROUP\b)', r') \1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'(\d)\s*(GROUP\b)', r'\1 \2', sql, flags=re.IGNORECASE)
    for kw in _MAJOR:
        pattern = r'(?i)(\S)(' + re.escape(kw) + r')'
        sql = re.sub(pattern, r'\1 \2', sql)
    sql = re.sub(r'[ \t]+', ' ', sql)
    sql = re.sub(r'\s+\n', '\n', sql)
    sql = re.sub(r'\n\s+', '\n', sql)
    return sql.strip()

def _clean_sql(raw: str) -> str:
    text = _strip_code_fences(raw)
    m = re.search(r'(?is)\bselect\b', text)
    if m:
        text = text[m.start():]
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("--")]
    text = "\n".join(lines).strip()
    text = text.rstrip("; \n\t")
    return _fix_keyword_glue(text)

def _wrap_sum_with_coalesce(sql: str) -> str:
    def repl(m):
        inner = m.group(1)
        if re.search(r'(?i)\bcoalesce\s*\(', inner):
            return m.group(0)
        return f"SUM(COALESCE({inner}, 0))"
    return re.sub(r'(?is)\bsum\s*\(\s*([^\)]+?)\s*\)', repl, sql)

def _columns_in_group_by(sql: str) -> List[str]:
    m = re.search(r'(?is)\bgroup\s+by\b(.*?)(?:\border\s+by\b|\blimit\b|$)', sql)
    if not m:
        return []
    tokens = [t.strip() for t in m.group(1).split(",") if t.strip()]
    cols = []
    for t in tokens:
        if re.fullmatch(r'\d+', t):
            continue
        mm = re.search(r'\"?([A-Za-z_][A-Za-z0-9_]*)\"?$', t)
        if mm:
            cols.append(mm.group(1))
    return cols

def _inject_non_blank_filter(sql: str, text_columns: List[str], dialect: str) -> str:
    gb_cols = _columns_in_group_by(sql)
    if not gb_cols:
        return sql
    dims = [c for c in gb_cols if c in set(text_columns)]
    if not dims:
        return sql

    predicate = " AND ".join(
        [f"\"{c}\" IS NOT NULL AND LENGTH(TRIM(\"{c}\")) > 0" for c in dims]
    )
    if re.search(r'(?is)\bwhere\b', sql):
        return re.sub(r'(?is)\bwhere\b', f"WHERE {predicate} AND ", sql, count=1)

    m = re.search(r'(?is)\bfrom\b\s+.+?(?=\bwhere\b|\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)', sql)
    if m:
        insert_at = m.end()
        return sql[:insert_at] + f" WHERE {predicate} " + sql[insert_at:]
    return sql + f" WHERE {predicate} "

def _apply_time_bucket_if_needed(sql: str,
                                 question: str,
                                 schema_metadata: Dict[str, List[Dict]],
                                 dialect: str) -> str:
    q = question.lower()
    bucket = None
    if re.search(r'\b(monthly|per month|by month|each month)\b', q): bucket = "month"
    elif re.search(r'\b(daily|per day|by day|each day)\b', q):       bucket = "day"
    elif re.search(r'\b(weekly|per week|by week|each week)\b', q):   bucket = "week"
    elif re.search(r'\b(quarterly|per quarter|by quarter)\b', q):    bucket = "quarter"
    elif re.search(r'\b(yearly|per year|by year|annual|annually)\b', q): bucket = "year"

    if not bucket or re.search(r'(?i)\bgroup\s+by\b', sql):
        return sql

    # find a date-like column
    date_cols: List[str] = []
    for _, cols in schema_metadata.items():
        for c in cols:
            ctype = str(c.get("type", "")).lower()
            name = c["name"]
            if ("date" in ctype) or ("timestamp" in ctype) or re.search(r'(date|month|day|year)$', name, re.IGNORECASE):
                date_cols.append(name)
    if not date_cols:
        return sql

    date_col = next((n for n in date_cols if n.lower() == "month"), date_cols[0])
    qcol = f"\"{date_col}\"" if not date_col.startswith('"') else date_col

    # ---- PERIOD AS STRING (NO TIMESTAMP) ----
    if dialect.lower().startswith("postgres"):
        # Always cast to DATE and render as ISO text to avoid tz/timestamp bleed-through
        mapping = {
            "day":     "TO_CHAR(DATE_TRUNC('day', {c})::date, 'YYYY-MM-DD')",
            "week":    "TO_CHAR(DATE_TRUNC('week', {c})::date, 'YYYY-MM-DD')",
            "month":   "TO_CHAR(DATE_TRUNC('month', {c})::date, 'YYYY-MM-DD')",
            "quarter": "TO_CHAR(DATE_TRUNC('quarter', {c})::date, 'YYYY-MM-DD')",
            "year":    "TO_CHAR(DATE_TRUNC('year', {c})::date, 'YYYY-MM-DD')",
        }
        bucket_expr = mapping[bucket].format(c=qcol)
    elif dialect.lower().startswith("sqlite"):
        # SQLite strftime already returns TEXT
        fmt = {"day": "%Y-%m-%d", "week": "%Y-%W-01", "month": "%Y-%m-01", "year": "%Y-01-01"}
        bucket_expr = f"strftime('{fmt.get(bucket, '%Y-%m-01')}', {qcol})"
    else:
        # Fallback: just cast to DATE where possible and stringify
        bucket_expr = f"CAST({qcol} AS DATE)"

    # Extract SUM(...) in SELECT; keep rest after FROM
    m = re.search(r'(?is)select\s+(.*?)\s+from\b', sql)
    sum_expr = "SUM(1)"
    rest = sql
    if m:
        select_part = m.group(1)
        sm = re.search(r'(?is)(sum\s*\([^\)]*\))', select_part)
        if sm:
            sum_expr = sm.group(1)
        rest = sql[m.end():]

    new_sql = f"SELECT {bucket_expr} AS period, {sum_expr} AS value FROM {rest}"

    if not re.search(r'(?i)\bgroup\s+by\b', new_sql):
        m_order = re.search(r'(?i)\border\s+by\b', new_sql)
        m_limit = re.search(r'(?i)\blimit\b', new_sql)
        cut = min([p for p in [m_order.start() if m_order else None,
                               m_limit.start() if m_limit else None]
                   if p is not None] or [len(new_sql)])
        new_sql = new_sql[:cut] + " GROUP BY 1 " + new_sql[cut:]
    if not re.search(r'(?i)\border\s+by\b', new_sql):
        new_sql += " ORDER BY 1"

    return new_sql

# -------------------- public API --------------------

def generate_sql(natural_language_query: str,
                 schema_metadata: Dict[str, List[Dict]],
                 dialect: str = "postgresql") -> str:
    prompt = _build_prompt(natural_language_query, schema_metadata, dialect)

    try:
        resp = _model.generate_content(prompt)
        raw = resp.text or ""
    except Exception as e:
        raise RuntimeError(f"Gemini API error: {e}")

    sql = _clean_sql(raw)
    sql = _wrap_sum_with_coalesce(sql)

    # collect text-like columns for dimension filter
    text_cols: List[str] = []
    for _, cols in schema_metadata.items():
        for c in cols:
            ctype = str(c.get("type", "")).lower()
            if "text" in ctype or "char" in ctype or "string" in ctype:
                text_cols.append(c["name"])

    if re.search(r"(?i)\bgroup\s+by\b", sql) or re.search(r"(?i)\bsum\s*\(", sql):
        sql = _inject_non_blank_filter(sql, text_cols, dialect)

    # rewrite to time buckets if the question asks for per-period results
    sql = _apply_time_bucket_if_needed(sql, natural_language_query, schema_metadata, dialect)

    sql = _clean_sql(sql)  # final spacing/cleanup
    return sql
