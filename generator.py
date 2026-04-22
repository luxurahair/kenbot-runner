"""
pipeline/client.py — Client OpenAI centralise.
Un seul endroit pour gerer la connexion OpenAI.
"""

import os
from typing import Optional

_client = None
_model_default = None


def get_openai_client():
    """Retourne le client OpenAI (singleton). None si pas de cle."""
    global _client
    if _client is not None:
        return _client
    try:
        from openai import OpenAI
    except ImportError:
        return None
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    _client = OpenAI(api_key=api_key)
    return _client


def get_default_model() -> str:
    """Retourne le modele par defaut (configurable via env)."""
    return os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def get_smart_model() -> str:
    """Retourne le modele pour llm_v3 (generation intelligente)."""
    return os.getenv("OPENAI_SMART_MODEL", "gpt-4o")


def chat_completion(
    system_prompt: str,
    user_message: str,
    model: Optional[str] = None,
    max_tokens: int = 300,
    temperature: float = 0.85,
    top_p: float = 0.95,
) -> Optional[str]:
    """
    Appel centralise a l'API OpenAI chat completions.
    Retourne le texte genere ou None si erreur.
    """
    client = get_openai_client()
    if not client:
        return None

    use_model = model or get_default_model()

    try:
        response = client.chat.completions.create(
            model=use_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as e:
        print(f"[PIPELINE ERROR] model={use_model} err={e}", flush=True)
        return None
