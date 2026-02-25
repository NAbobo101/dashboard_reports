import json
import time
from typing import Any, Dict, Optional

import requests

API_BASE = "https://api.mercadolibre.com"


class MeliApiError(RuntimeError):
    def __init__(self, message: str, status_code: int = 0, payload: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


def _safe_json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except Exception:
        # às vezes o ML retorna HTML/texto em erros intermediários
        return {"raw": resp.text[:2000]}


def _retry_after_seconds(resp: requests.Response) -> Optional[int]:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return int(ra)
    except Exception:
        return None


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
    timeout: int = 25,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Request com retries para falhas transitórias.
    - Retry em: 429, 5xx, timeouts/connection errors.
    - Não retry em 4xx (exceto 429).
    """
    attempt = 0
    last_exc: Optional[Exception] = None

    while attempt <= max_retries:
        try:
            resp = session.request(
                method=method,
                url=url,
                headers=headers,
                data=data,
                timeout=timeout,
            )

            # Rate limit
            if resp.status_code == 429 and attempt < max_retries:
                wait = _retry_after_seconds(resp)
                if wait is None:
                    wait = min(2 ** attempt, 10)  # 1,2,4,8.. cap 10s
                time.sleep(wait)
                attempt += 1
                continue

            # Erros 5xx (transitórios)
            if 500 <= resp.status_code <= 599 and attempt < max_retries:
                time.sleep(min(2 ** attempt, 10))
                attempt += 1
                continue

            # Demais: processa resultado
            payload = _safe_json(resp)

            if 200 <= resp.status_code <= 299:
                return payload

            # 4xx/5xx final: levanta erro com contexto
            msg = payload.get("message") or payload.get("error") or f"HTTP {resp.status_code}"
            raise MeliApiError(msg, status_code=resp.status_code, payload=payload)

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(min(2 ** attempt, 10))
                attempt += 1
                continue
            raise MeliApiError(f"Falha de rede/timeout ao chamar {url}: {e}") from e

        except MeliApiError:
            raise

        except Exception as e:
            # Erro inesperado
            raise MeliApiError(f"Erro inesperado ao chamar {url}: {e}") from e

    # fallback (não deve chegar aqui)
    raise MeliApiError(f"Falha ao chamar {url}", payload={"exception": str(last_exc) if last_exc else ""})


def exchange_code(
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    code: str,
    code_verifier: str,
    *,
    session: Optional[requests.Session] = None,
) -> dict:
    """
    Troca authorization_code por access_token/refresh_token.
    """
    if not all([client_id, client_secret, redirect_uri, code, code_verifier]):
        raise ValueError("Parâmetros obrigatórios faltando em exchange_code()")

    s = session or requests.Session()
    return _request_json(
        s,
        "POST",
        f"{API_BASE}/oauth/token",
        headers={"Accept": "application/json"},
        data={
            "grant_type": "authorization_code",
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
            "code_verifier": code_verifier,
        },
        timeout=25,
        max_retries=2,  # token exchange: retries curtos
    )


def refresh_token(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    *,
    session: Optional[requests.Session] = None,
) -> dict:
    """
    Renova access_token via refresh_token (refresh pode rotacionar).
    """
    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("Parâmetros obrigatórios faltando em refresh_token()")

    s = session or requests.Session()
    return _request_json(
        s,
        "POST",
        f"{API_BASE}/oauth/token",
        headers={"Accept": "application/json"},
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        },
        timeout=25,
        max_retries=2,
    )


def users_me(access_token: str, *, session: Optional[requests.Session] = None) -> dict:
    """
    Retorna dados do usuário autenticado (/users/me).
    """
    if not access_token:
        raise ValueError("access_token vazio em users_me()")

    s = session or requests.Session()
    return _request_json(
        s,
        "GET",
        f"{API_BASE}/users/me",
        headers={"Accept": "application/json", "Authorization": f"Bearer {access_token}"},
        timeout=20,
        max_retries=3,
    )
