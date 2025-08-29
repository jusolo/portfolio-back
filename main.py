import os
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import aiofiles
from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.ia import ask_ai  # ahora apunta a app/ia.py

from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# Configuración
# -----------------------------------------------------------------------------
ROOT_PATH = os.getenv("FASTAPI_ROOT_PATH", "/api/v1.0")
LOGGER_PATH = Path(os.environ.get("LOGGER_PATH", "logs/logger.txt"))
CORS_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*").split(",")
BOGOTA_TZ = ZoneInfo("America/Bogota")

app = FastAPI(title="UNP Chat API", root_path=ROOT_PATH)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ORIGINS if o.strip()] or ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("unp-chat")

# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------
class QuestionIn(BaseModel):
    question: str = Field(..., min_length=1, description="Pregunta del usuario")

class AnswerOut(BaseModel):
    answer: str

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
async def append_logger(question: str, answer: str) -> None:
    ts = datetime.now(BOGOTA_TZ).isoformat(timespec="seconds")
    entry = (
        f"---\n"
        f"time: {ts}\n"
        f"question: {question.strip() or '(vacía)'}\n"
        f"answer: {answer.strip() or '(vacía)'}\n"
    )
    LOGGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(LOGGER_PATH, "a", encoding="utf-8") as f:
        await f.write(entry)

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok", "tz": "America/Bogota"}

@app.post("/quest", response_model=AnswerOut, tags=["qa"])
async def quest(payload: QuestionIn, background: BackgroundTasks):
    question = payload.question.strip()

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

    background.add_task(append_logger, question, answer)
    return JSONResponse(content=AnswerOut(answer=answer).model_dump())
