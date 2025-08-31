# app/storage/qa_log_pg.py
import asyncio
from typing import Optional, Dict, Any
from psycopg.rows import dict_row
from .pg import POOL

INIT_SQL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
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

async def init_db(retries: int = 5, delay: float = 2.0):
    for i in range(retries):
        try:
            async with POOL.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(INIT_SQL)
            return
        except Exception as e:
            if i == retries - 1:
                # última: no mates la app; lo intentaremos en la primera escritura
                print(f"[qa_logs] init_db falló: {e}")
                return
            await asyncio.sleep(delay * (i + 1))

async def log_qa(question: str, answer: str, source: str = "ai", tz: str = "America/Bogota", meta: Optional[Dict[str, Any]] = None):
    try:
        async with POOL.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "INSERT INTO qa_logs (question, answer, source, tz, meta) VALUES (%s, %s, %s, %s, %s)",
                    (question, answer, source, tz, meta),
                )
    except Exception as e:
        # no rompas la petición por logging
        print(f"[qa_logs] write fallo: {e}")
