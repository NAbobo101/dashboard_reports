from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from db_upsert import mysql_connect

# Tenta importar validate_verifier do módulo pkce (com e sem pacote)
try:
    from .pkce import validate_verifier  # type: ignore
except Exception:  # pragma: no cover
    from pkce import validate_verifier  # type: ignore


# ----------------------------
# Erros tipados (facilita mapear p/ HTTP)
# ----------------------------

class OAuthStateError(Exception):
    """Base para erros relacionados a state/PKCE."""


class InvalidState(OAuthStateError):
    """State não existe (ou foi apagado)."""


class StateAlreadyUsed(OAuthStateError):
    """State já foi consumido (replay)."""


class StateExpired(OAuthStateError):
    """State expirou (TTL passou)."""


@dataclass(frozen=True)
class StateRow:
    code_verifier: str
    expires_at: datetime
    used_at: Optional[datetime]


# ----------------------------
# Helpers
# ----------------------------

def _utcnow_naive() -> datetime:
    """
    Retorna UTC 'naive' (sem tzinfo). Útil porque MySQL costuma voltar datetime naive.
    """
    return datetime.utcnow()


def _coerce_dt_naive_utc(dt: Any) -> datetime:
    """
    Normaliza datetime vindo do driver:
    - Se vier aware, converte para UTC naive.
    - Se vier naive, assume UTC.
    """
    if isinstance(dt, datetime):
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    raise ValueError("expires_at inválido (não é datetime)")


def _fetchone_row(cur) -> Optional[Any]:
    """
    Suporta cursor que retorna tuple ou dict.
    """
    return cur.fetchone()


def _parse_state_row(row: Any) -> StateRow:
    if row is None:
        raise InvalidState("invalid_state")

    # dict cursor
    if isinstance(row, dict):
        verifier = row.get("code_verifier")
        expires_at = row.get("expires_at")
        used_at = row.get("used_at")
    else:
        # tuple cursor: (code_verifier, expires_at, used_at)
        try:
            verifier, expires_at, used_at = row
        except Exception as e:
            raise ValueError("Row inesperada em meli_oauth_states") from e

    verifier = str(verifier or "")
    expires_at_dt = _coerce_dt_naive_utc(expires_at)
    used_at_dt = _coerce_dt_naive_utc(used_at) if isinstance(used_at, datetime) else None

    return StateRow(code_verifier=verifier, expires_at=expires_at_dt, used_at=used_at_dt)


def _begin_transaction(conn, cur) -> None:
    """
    Garante transação para que SELECT ... FOR UPDATE funcione corretamente.
    """
    # Alguns drivers têm conn.begin()
    begin = getattr(conn, "begin", None)
    if callable(begin):
        begin()
        return
    # Fallback universal
    cur.execute("START TRANSACTION")


def _safe_close(cur, conn) -> None:
    try:
        if cur is not None:
            cur.close()
    except Exception:
        pass
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass


# ----------------------------
# API: state storage
# ----------------------------

def save_state(state_hash: str, code_verifier: str, expires_at: datetime, requester: str = "") -> None:
    """
    Salva state_hash + code_verifier (PKCE) com TTL.
    - expires_at deve ser UTC (aceita aware e converte).
    - NÃO salva o state em claro: apenas hash.
    """
    if not state_hash or len(state_hash) != 64:
        raise ValueError("state_hash inválido (esperado sha256 hex 64)")
    validate_verifier(code_verifier)

    exp = _coerce_dt_naive_utc(expires_at)

    conn = mysql_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO meli_oauth_states (state_hash, code_verifier, expires_at, requester)
            VALUES (%s, %s, %s, %s)
            """,
            (state_hash, code_verifier, exp, (requester or "")[:255]),
        )
        conn.commit()
    except Exception:
        # Importante: não deixar transação pendurada
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _safe_close(cur, conn)


def pop_state(state_hash: str) -> str:
    """
    Consome state de forma atômica:
    - valida existência
    - valida used_at (one-time use)
    - valida expires_at (TTL)
    - marca used_at=UTC_TIMESTAMP()
    - retorna code_verifier
    """
    if not state_hash or len(state_hash) != 64:
        raise ValueError("state_hash inválido (esperado sha256 hex 64)")

    conn = mysql_connect()
    cur = conn.cursor()
    try:
        _begin_transaction(conn, cur)

        cur.execute(
            """
            SELECT code_verifier, expires_at, used_at
            FROM meli_oauth_states
            WHERE state_hash = %s
            FOR UPDATE
            """,
            (state_hash,),
        )
        row = _fetchone_row(cur)
        st = _parse_state_row(row)

        if st.used_at is not None:
            conn.rollback()
            raise StateAlreadyUsed("state_already_used")

        now = _utcnow_naive()
        if st.expires_at <= now:
            conn.rollback()
            raise StateExpired("state_expired")

        # Marca como usado (one-time use)
        cur.execute(
            "UPDATE meli_oauth_states SET used_at = UTC_TIMESTAMP() WHERE state_hash = %s",
            (state_hash,),
        )
        conn.commit()

        # Verificação final do verifier
        validate_verifier(st.code_verifier)
        return st.code_verifier

    except (InvalidState, StateAlreadyUsed, StateExpired):
        # Já cuidamos commit/rollback acima conforme caso
        raise
    except Exception:
        # qualquer erro inesperado: rollback
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _safe_close(cur, conn)


def cleanup_states(max_age_days: int = 30) -> int:
    """
    Limpeza opcional: remove states usados e expirados antigos.
    - Remove:
      - usados há mais de max_age_days
      - expirados há mais de max_age_days
    Retorna número de linhas removidas (quando o driver suporta rowcount).
    """
    if max_age_days < 1:
        max_age_days = 1

    conn = mysql_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM meli_oauth_states
            WHERE
              (used_at IS NOT NULL AND used_at < (UTC_TIMESTAMP() - INTERVAL %s DAY))
              OR
              (used_at IS NULL AND expires_at < (UTC_TIMESTAMP() - INTERVAL %s DAY))
            """,
            (max_age_days, max_age_days),
        )
        deleted = getattr(cur, "rowcount", 0) or 0
        conn.commit()
        return int(deleted)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _safe_close(cur, conn)
