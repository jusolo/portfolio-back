import os
import logging
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from app.ia import ask_ai
from app.storage import qa_cache_pg as qa_cache
from app.storage.pg import open_pool, close_pool
from app.storage.qa_log_pg import init_db as init_logs_db, log_qa

load_dotenv()

# -----------------------------------------------------------------------------
# Configuración
# -----------------------------------------------------------------------------
QA_CACHE_MAX_AGE_DAYS = int(os.getenv("QA_CACHE_MAX_AGE_DAYS", "365"))
QA_CACHE_FUZZY = os.getenv("QA_CACHE_FUZZY", "1") == "1"
QA_CACHE_SIM = int(os.getenv("QA_CACHE_SIM", "92"))

ROOT_PATH = os.getenv("FASTAPI_ROOT_PATH", "/api/v1.0")
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ALLOW_ORIGINS", "*").split(",") if o.strip()]
BOGOTA_TZ = ZoneInfo("America/Bogota")

app = FastAPI(title="sebastian ospina API", root_path=ROOT_PATH)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("qa-api")

# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------
class QuestionIn(BaseModel):
    question: str = Field(..., min_length=1, description="Pregunta del usuario")

class AnswerOut(BaseModel):
    answer: str

# -----------------------------------------------------------------------------
# Lifecycle
# -----------------------------------------------------------------------------
@app.on_event("startup")
async def _startup():
    # Pool + tablas de logs
    await open_pool()
    await init_logs_db()
    # Tu caché (sqlite o lo que tengas ahora)
    await qa_cache.init_db()

@app.on_event("shutdown")
async def _shutdown():
    await close_pool()

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok", "tz": "America/Bogota"}

@app.post("/quest", response_model=AnswerOut, tags=["qa"])
async def quest(payload: QuestionIn, background: BackgroundTasks):
    question = payload.question.strip()

    if not question:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se envió ninguna pregunta")

    # * 1) caché exacta
    cached = await qa_cache.get_exact(question, max_age_days=QA_CACHE_MAX_AGE_DAYS)
    if not cached and QA_CACHE_FUZZY:
        cached = await qa_cache.get_fuzzy(question, similarity=QA_CACHE_SIM, max_age_days=QA_CACHE_MAX_AGE_DAYS)
    if cached:
        # Log a BD sin bloquear
        asyncio.create_task(log_qa(question, cached["answer"], source="cache"))
        return JSONResponse({"answer": cached["answer"], "cached": True})

    # * 2) IA
    try:
        answer = await ask_ai(question)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Fallo generando respuesta")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Fallo generando respuesta",
        ) from e
    
    # * 3) persistir en caché
    try:
        await qa_cache.put(question, answer, model=os.getenv("GENAI_MODEL", "gemini"))
    except Exception:
        logger.warning("No se pudo guardar en qa_cache", exc_info=True)

    # * 4) log a BD sin bloquear
    asyncio.create_task(log_qa(question, answer, source="ai"))

    return JSONResponse(content=AnswerOut(answer=answer).model_dump())
