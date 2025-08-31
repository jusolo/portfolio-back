# app/storage/qa_cache_pg.py
import hashlib
import json
import re
import unicodedata
from typing import Optional, Dict, Any, List
from psycopg.rows import dict_row
from .pg import POOL  # AsyncConnectionPool configurado en app/storage/pg.py

# ----------------- normalización y hash -----------------
_norm_ws = re.compile(r"\s+")
_norm_punct = re.compile(r"[^\w\s]", re.UNICODE)

def normalize(text: str) -> str:
    t = text.strip().lower()
    t = unicodedata.normalize("NFD", t)
    t = "".join(ch for ch in t if unicodedata.category(ch) != "Mn")
    t = _norm_punct.sub(" ", t)
    t = _norm_ws.sub(" ", t).strip()
    return t

def qhash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# ----------------- DDL -----------------
INIT_SQL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS qa_cache (
  qhash TEXT PRIMARY KEY,
  question_norm TEXT NOT NULL,
  question_original TEXT NOT NULL,
  answer TEXT NOT NULL,
  model TEXT,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz NOT NULL DEFAULT now(),
  hits INTEGER NOT NULL DEFAULT 1,
  meta JSONB
);

CREATE INDEX IF NOT EXISTS idx_qa_created     ON qa_cache(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_qa_norm_trgm   ON qa_cache USING GIN (question_norm gin_trgm_ops);
"""

async def init_db() -> None:
    async with POOL.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(INIT_SQL)

# ----------------- operaciones -----------------
async def get_exact(question: str, max_age_days: Optional[int] = None) -> Optional[Dict[str, Any]]:
    n = normalize(question)
    h = qhash(n)

    base_sql = "SELECT * FROM qa_cache WHERE qhash=%s"
    params: List[Any] = [h]

    if max_age_days is not None:
        base_sql += " AND created_at >= now() - (%s || ' days')::interval"
        params.append(str(max_age_days))

    async with POOL.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(base_sql, params)
            row = await cur.fetchone()
            if not row:
                return None
            await cur.execute("UPDATE qa_cache SET hits=hits+1, last_used_at=now() WHERE qhash=%s", (h,))
            return dict(row)

async def put(question: str, answer: str, model: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> None:
    n = normalize(question)
    h = qhash(n)
    meta_json = json.dumps(meta or {}, ensure_ascii=False)

    async with POOL.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO qa_cache (qhash, question_norm, question_original, answer, model, meta)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (qhash) DO UPDATE SET
                  last_used_at = now(),
                  hits = qa_cache.hits + 1
                """,
                (h, n, question, answer, model, meta_json),
            )

# similarity: acepta 92 o 0.92
def _sim_float(similarity: float | int) -> float:
    try:
        s = float(similarity)
    except Exception:
        return 0.92
    return s / 100.0 if s > 1.0 else s

async def get_fuzzy(question: str, similarity: float | int = 0.92, max_age_days: Optional[int] = None) -> Optional[Dict[str, Any]]:
    n = normalize(question)
    s = _sim_float(similarity)

    where = ["similarity(question_norm, %s) >= %s"]
    params: List[Any] = [n, s]

    if max_age_days is not None:
        where.append("created_at >= now() - (%s || ' days')::interval")
        params.append(str(max_age_days))

    sql = f"""
      SELECT *, similarity(question_norm, %s) AS sim
      FROM qa_cache
      WHERE {" AND ".join(where)}
      ORDER BY sim DESC, last_used_at DESC
      LIMIT 1
    """

    async with POOL.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            row = await cur.fetchone()
            if not row:
                return None
            await cur.execute("UPDATE qa_cache SET hits=hits+1, last_used_at=now() WHERE qhash=%s", (row["qhash"],))
            return dict(row)

async def search(term: str, limit: int = 20) -> List[Dict[str, Any]]:
    n = normalize(term)
    async with POOL.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT question_original, answer, hits, last_used_at,
                       similarity(question_norm, %s) AS sim
                FROM qa_cache
                WHERE similarity(question_norm, %s) >= 0.3
                ORDER BY sim DESC, last_used_at DESC
                LIMIT %s
                """,
                (n, n, limit),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

async def invalidate(question: str) -> int:
    n = normalize(question)
    h = qhash(n)
    async with POOL.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM qa_cache WHERE qhash=%s", (h,))
            return cur.rowcount or 0
