from __future__ import annotations

import os
import textwrap
from pathlib import Path
from typing import Optional, List

from fastapi import HTTPException, status

try:
    from google import genai
except Exception:
    genai = None

# -----------------------------------------------------------------------------
# Path del contexto
# -----------------------------------------------------------------------------
CONTEXT_DIR = Path(__file__).resolve().parent / "context"

# -----------------------------------------------------------------------------
# Carga de contexto
# -----------------------------------------------------------------------------
def load_context(path: Optional[Path] = None) -> str:
    """
    Carga todo el contenido .txt en app/context (o en el path dado).
    Une todo en un único string que se pasará como contexto al prompt.
    """
    directory = path or CONTEXT_DIR
    if not directory.exists():
        return ""

    texts: List[str] = []
    for file in sorted(directory.glob("*.txt")):
        try:
            text = file.read_text(encoding="utf-8").strip()
            if text:
                texts.append(f"[{file.stem.upper()}]\n{text}")
        except Exception:
            continue

    return "\n\n".join(texts)

def build_prompt(query: str, context: str) -> str:
    return f"""
    Eres un asistente personal. 
    Responde siempre en ESPAÑOL, con un tono natural y cercano, como si conversarás con una persona.

    INSTRUCCIONES:
    - Usa exclusivamente la información provista en CONTEXTO.
    - Sé breve, claro y directo.
    - Si la pregunta no está cubierta en el contexto, responde:
    "No tengo esa información en el contexto disponible."
    - No inventes datos externos.

    PREGUNTA DEL USUARIO:
    {query}

    CONTEXTO:
    {context}
        """.strip()


# -----------------------------------------------------------------------------
# Cliente Gemini
# -----------------------------------------------------------------------------
def _get_client():
    if genai is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Dependencia genai no disponible",
        )

    api_key = os.getenv("GOOGLE_GENAI_API_KEY")
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Falta configurar GOOGLE_GENAI_API_KEY",
        )

    return genai.Client(api_key=api_key)

# -----------------------------------------------------------------------------
# Entrada pública
# -----------------------------------------------------------------------------
async def ask_ai(user_text: str) -> str:
    if not user_text:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pregunta vacía",
        )

    context = load_context()
    prompt = build_prompt(user_text, context)
    client = _get_client()

    # intento con API nueva
    try:
        resp = client.responses.generate(
            model=os.getenv("GENAI_MODEL", "gemini-2.5-flash"),
            input=prompt,
        )
        if hasattr(resp, "output_text") and resp.output_text:
            return resp.output_text.strip()
    except Exception:
        pass

    # fallback API vieja
    try:
        resp = client.models.generate_content(
            model=os.getenv("GENAI_MODEL", "gemini-2.5-flash"),
            contents=prompt,
        )
        text = getattr(resp, "text", None) or str(resp)
        return text.strip()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Error al procesar con IA",
        )
