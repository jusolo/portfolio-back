from typing import Optional, Dict, Any
from psycopg.rows import dict_row
from .pg import POOL

INIT_SQL = """
CREATE TABLE IF NOT EXISTS qa_logs (
  id BIGSERIAL PRIMARY KEY,
  asked_at timestamptz NOT NULL DEFAULT now(),
  tz TEXT NOT NULL DEFAULT 'America/Bogota',
  question TEXT NOT NULL,
  answer TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('cache','ai')),
  meta JSONB
);
CREATE INDEX IF NOT EXISTS idx_qa_logs_asked_at ON qa_logs(asked_at DESC);
"""

async def init_db():
    async with POOL.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(INIT_SQL)

async def log_qa(question: str, answer: str, source: str = "ai", tz: str = "America/Bogota", meta: Optional[Dict[str, Any]] = None):
    async with POOL.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "INSERT INTO qa_logs (question, answer, source, tz, meta) VALUES (%s, %s, %s, %s, %s)",
                (question, answer, source, tz, meta)
            )
