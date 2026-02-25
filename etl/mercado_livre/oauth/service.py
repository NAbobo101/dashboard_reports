import hashlib
import hmac
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from fastapi import Body, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from .pkce import make_challenge, make_state, make_verifier, validate_state, validate_verifier
from .meli_client import MeliApiError, exchange_code, refresh_token as meli_refresh_token, users_me

logger = logging.getLogger("meli_oauth_service")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

# ----------------------------
# Config (env)
# ----------------------------
INTERNAL_KEY = os.environ.get("MELI_INTERNAL_KEY", "")  # chave compartilhada WP -> /relatorios (server-to-server)

MELI_CLIENT_ID = os.environ.get("MELI_CLIENT_ID", "") or os.environ.get("MELI_APP_ID", "")
MELI_CLIENT_SECRET = os.environ.get("MELI_CLIENT_SECRET", "")
MELI_REDIRECT_URI = os.environ.get("MELI_REDIRECT_URI", "")
MELI_SCOPE = os.environ.get("MELI_SCOPE", "offline_access read write")

# URL de autorização (BR padrão)
MELI_AUTH_URL = os.environ.get("MELI_AUTH_URL", "https://auth.mercadolivre.com.br/authorization")

# DB
MYSQL_HOST = os.environ.get("MYSQL_HOST", "db")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "mercado_livre")

# Segurança / comportamento
STATE_TTL_MINUTES = int(os.environ.get("MELI_STATE_TTL_MINUTES", "10"))
TOKEN_EXPIRY_SKEW_SECONDS = int(os.environ.get("MELI_TOKEN_SKEW_SECONDS", "60"))  # refresh antes de expirar


# ----------------------------
# MySQL driver (pymysql preferido)
# ----------------------------
def _mysql_connect():
    """
    Retorna uma conexão DB-API 2.0 (pymysql ou mysql.connector).
    """
    try:
        import pymysql  # type: ignore
        return pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset="utf8mb4",
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )
    except ImportError:
        pass

    try:
        import mysql.connector  # type: ignore
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            autocommit=False,
        )
    except ImportError as e:
        raise RuntimeError(
            "Nenhum driver MySQL disponível. Instale 'pymysql' (recomendado) ou 'mysql-connector-python'."
        ) from e


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _require_config():
    missing = []
    if not INTERNAL_KEY:
        missing.append("MELI_INTERNAL_KEY")
    if not MELI_CLIENT_ID:
        missing.append("MELI_CLIENT_ID (ou MELI_APP_ID)")
    if not MELI_CLIENT_SECRET:
        missing.append("MELI_CLIENT_SECRET")
    if not MELI_REDIRECT_URI:
        missing.append("MELI_REDIRECT_URI")
    if not MYSQL_USER:
        missing.append("MYSQL_USER")
    if not MYSQL_PASSWORD:
        missing.append("MYSQL_PASSWORD")
    if missing:
        raise HTTPException(status_code=500, detail={"error": "config_missing", "missing": missing})


def _check_internal_key(x_internal_key: str) -> None:
    # comparação constant-time
    if not INTERNAL_KEY or not x_internal_key or not hmac.compare_digest(INTERNAL_KEY, x_internal_key):
        raise HTTPException(status_code=401, detail={"error": "unauthorized"})


# ----------------------------
# DB ops: states
# ----------------------------
def db_save_state(state_hash: str, code_verifier: str, expires_at: datetime, requester: str) -> None:
    conn = _mysql_connect()
    try:
        cur = conn.cursor()
        # compat: cursor pode ser dict ou tuple, não importa aqui
        cur.execute(
            """
            INSERT INTO meli_oauth_states (state_hash, code_verifier, expires_at, requester)
            VALUES (%s, %s, %s, %s)
            """,
            (
                state_hash,
                code_verifier,
                expires_at.replace(tzinfo=None),
                requester[:255],
            ),
        )
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def db_pop_state_verifier(state_hash: str) -> str:
    """
    Valida: existe, não expirou, não foi usado. Marca used_at e retorna code_verifier.
    Usa SELECT ... FOR UPDATE para evitar corrida.
    """
    conn = _mysql_connect()
    try:
        cur = conn.cursor()
        cur.execute("START TRANSACTION")
        cur.execute(
            """
            SELECT code_verifier, expires_at, used_at
            FROM meli_oauth_states
            WHERE state_hash = %s
            FOR UPDATE
            """,
            (state_hash,),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(status_code=400, detail={"error": "invalid_or_expired_state"})

        # row pode vir como dict (pymysql) ou tuple (mysql.connector)
        if isinstance(row, dict):
            verifier = row.get("code_verifier")
            expires_at = row.get("expires_at")
            used_at = row.get("used_at")
        else:
            verifier, expires_at, used_at = row

        if used_at is not None:
            conn.rollback()
            raise HTTPException(status_code=400, detail={"error": "state_already_used"})

        now_utc = _utcnow().replace(tzinfo=None)
        if expires_at is None or expires_at <= now_utc:
            conn.rollback()
            raise HTTPException(status_code=400, detail={"error": "invalid_or_expired_state"})

        cur.execute(
            "UPDATE meli_oauth_states SET used_at = UTC_TIMESTAMP() WHERE state_hash = %s",
            (state_hash,),
        )
        conn.commit()

        verifier = str(verifier or "")
        validate_verifier(verifier)
        return verifier
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ----------------------------
# DB ops: tokens / sellers
# (Assume tabelas: meli_tokens, meli_sellers conforme seu roadmap)
# ----------------------------
def db_upsert_seller(seller_id: str, me: Dict[str, Any]) -> None:
    conn = _mysql_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO meli_sellers (seller_id, nickname, site_id, email, raw_json)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              nickname=VALUES(nickname),
              site_id=VALUES(site_id),
              email=VALUES(email),
              raw_json=VALUES(raw_json)
            """,
            (
                seller_id,
                me.get("nickname"),
                me.get("site_id"),
                me.get("email"),
                _safe_json_dump(me),
            ),
        )
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def db_upsert_tokens(seller_id: str, token_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Salva access/refresh e expires_at.
    Retorna (access_token, expires_at).
    """
    access_token = str(token_payload.get("access_token") or "")
    refresh_tok = str(token_payload.get("refresh_token") or "")
    token_type = token_payload.get("token_type")
    scope = token_payload.get("scope")

    expires_in = int(token_payload.get("expires_in") or 0)
    now = _utcnow()
    expires_at = now + timedelta(seconds=expires_in if expires_in > 0 else 3600)

    if not access_token or not refresh_tok:
        raise HTTPException(status_code=400, detail={"error": "token_payload_invalid"})

    conn = _mysql_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO meli_tokens (seller_id, access_token, refresh_token, token_type, scope, obtained_at, expires_at, last_refresh_at)
            VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP(), %s, UTC_TIMESTAMP())
            ON DUPLICATE KEY UPDATE
              access_token=VALUES(access_token),
              refresh_token=VALUES(refresh_token),
              token_type=VALUES(token_type),
              scope=VALUES(scope),
              expires_at=VALUES(expires_at),
              last_refresh_at=UTC_TIMESTAMP()
            """,
            (
                seller_id,
                access_token,
                refresh_tok,
                token_type,
                scope,
                expires_at.replace(tzinfo=None),
            ),
        )
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"access_token": access_token, "expires_at": int(expires_at.timestamp())}


def db_get_tokens_for_update(seller_id: str):
    """
    Lê tokens com lock (FOR UPDATE) dentro de transação.
    Retorna (conn, cur, row_dict)
    """
    conn = _mysql_connect()
    cur = conn.cursor()
    cur.execute("START TRANSACTION")
    cur.execute(
        """
        SELECT seller_id, access_token, refresh_token, token_type, scope, expires_at
        FROM meli_tokens
        WHERE seller_id = %s
        FOR UPDATE
        """,
        (seller_id,),
    )
    row = cur.fetchone()
    if not row:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail={"error": "not_connected"})

    if isinstance(row, dict):
        return conn, cur, row
    # mysql.connector tuple fallback
    keys = ["seller_id", "access_token", "refresh_token", "token_type", "scope", "expires_at"]
    return conn, cur, {k: v for k, v in zip(keys, row)}


def db_update_tokens_in_tx(cur, seller_id: str, token_payload: Dict[str, Any]) -> Dict[str, Any]:
    access_token = str(token_payload.get("access_token") or "")
    refresh_tok = str(token_payload.get("refresh_token") or "")
    token_type = token_payload.get("token_type")
    scope = token_payload.get("scope")

    expires_in = int(token_payload.get("expires_in") or 0)
    now = _utcnow()
    expires_at = now + timedelta(seconds=expires_in if expires_in > 0 else 3600)

    if not access_token or not refresh_tok:
        raise HTTPException(status_code=400, detail={"error": "refresh_payload_invalid"})

    cur.execute(
        """
        UPDATE meli_tokens
        SET access_token=%s,
            refresh_token=%s,
            token_type=%s,
            scope=%s,
            expires_at=%s,
            last_refresh_at=UTC_TIMESTAMP()
        WHERE seller_id=%s
        """,
        (access_token, refresh_tok, token_type, scope, expires_at.replace(tzinfo=None), seller_id),
    )
    return {"access_token": access_token, "expires_at": int(expires_at.timestamp())}


def _safe_json_dump(obj: Any) -> str:
    # evita importar json no topo com shadow, simples e seguro
    import json
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="Mercado Livre OAuth Service", version="1.0.0")


class ConsumePayload(BaseModel):
    code: str = Field(..., min_length=1)
    state: str = Field(..., min_length=1)


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/internal/meli/oauth/init")
def oauth_init(request: Request, x_internal_key: str = Header(default="")):
    _require_config()
    _check_internal_key(x_internal_key)

    state = make_state()
    validate_state(state)

    verifier = make_verifier()  # default 64 chars
    validate_verifier(verifier)

    challenge = make_challenge(verifier)
    state_hash = _sha256_hex(state)

    expires_at = _utcnow() + timedelta(minutes=STATE_TTL_MINUTES)
    requester = f"{request.client.host if request.client else ''} {request.headers.get('user-agent','')}".strip()

    db_save_state(state_hash=state_hash, code_verifier=verifier, expires_at=expires_at, requester=requester)

    qs = urlencode(
        {
            "response_type": "code",
            "client_id": MELI_CLIENT_ID,
            "redirect_uri": MELI_REDIRECT_URI,
            "state": state,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "scope": MELI_SCOPE,
        }
    )

    return {"authorization_url": f"{MELI_AUTH_URL}?{qs}"}


@app.post("/internal/meli/oauth/consume")
def oauth_consume(payload: ConsumePayload = Body(...), x_internal_key: str = Header(default="")):
    _require_config()
    _check_internal_key(x_internal_key)

    # valida state formato
    validate_state(payload.state)

    state_hash = _sha256_hex(payload.state)
    verifier = db_pop_state_verifier(state_hash)

    # Exchange code -> tokens
    try:
        tok = exchange_code(
            client_id=MELI_CLIENT_ID,
            client_secret=MELI_CLIENT_SECRET,
            redirect_uri=MELI_REDIRECT_URI,
            code=payload.code,
            code_verifier=verifier,
        )
    except MeliApiError as e:
        # não logar code nem tokens
        logger.warning("token_exchange_failed status=%s payload=%s", getattr(e, "status_code", 0), getattr(e, "payload", {}))
        raise HTTPException(status_code=400, detail={"error": "token_exchange_failed", "status": getattr(e, "status_code", 0), "detail": getattr(e, "payload", {})})
    except Exception as e:
        logger.exception("token_exchange_unexpected")
        raise HTTPException(status_code=500, detail={"error": "token_exchange_unexpected"})

    # Discover seller
    try:
        me = users_me(tok["access_token"])
        seller_id = str(me["id"])
    except MeliApiError as e:
        logger.warning("users_me_failed status=%s payload=%s", getattr(e, "status_code", 0), getattr(e, "payload", {}))
        raise HTTPException(status_code=400, detail={"error": "users_me_failed", "status": getattr(e, "status_code", 0), "detail": getattr(e, "payload", {})})
    except Exception:
        logger.exception("users_me_unexpected")
        raise HTTPException(status_code=500, detail={"error": "users_me_unexpected"})

    # Persist
    db_upsert_seller(seller_id, me)
    saved = db_upsert_tokens(seller_id, tok)

    return {"ok": True, "seller_id": seller_id, "expires_at": saved["expires_at"]}


@app.get("/internal/meli/token")
def get_valid_token(seller_id: str, x_internal_key: str = Header(default="")):
    """
    Retorna access_token válido; renova automaticamente se expirado/quase expirando.
    Protegido por X-Internal-Key.
    """
    _require_config()
    _check_internal_key(x_internal_key)

    seller_id = "".join([c for c in str(seller_id) if c.isdigit()])
    if not seller_id:
        raise HTTPException(status_code=400, detail={"error": "missing_seller_id"})

    conn, cur, row = db_get_tokens_for_update(seller_id)
    try:
        access = str(row.get("access_token") or "")
        refresh_tok = str(row.get("refresh_token") or "")
        expires_at = row.get("expires_at")

        # Normaliza expires_at (pode vir datetime)
        exp_ts = 0
        if isinstance(expires_at, datetime):
            exp_ts = int(expires_at.replace(tzinfo=timezone.utc).timestamp())
        elif expires_at is not None:
            # fallback: tenta parse/convert
            try:
                exp_ts = int(expires_at)
            except Exception:
                exp_ts = 0

        now_ts = int(_utcnow().timestamp())

        # Se ainda válido, devolve
        if access and exp_ts > (now_ts + TOKEN_EXPIRY_SKEW_SECONDS):
            conn.commit()
            return {"seller_id": seller_id, "access_token": access, "expires_at": exp_ts}

        if not refresh_tok:
            conn.rollback()
            raise HTTPException(status_code=400, detail={"error": "not_connected"})

        # Refresh (com lock do FOR UPDATE)
        try:
            tok = meli_refresh_token(
                client_id=MELI_CLIENT_ID,
                client_secret=MELI_CLIENT_SECRET,
                refresh_token=refresh_tok,
            )
        except MeliApiError as e:
            # invalid_grant -> precisa reautorizar
            payload = getattr(e, "payload", {}) or {}
            status = getattr(e, "status_code", 0)

            logger.warning("refresh_failed status=%s payload=%s", status, payload)

            if status == 400 and (payload.get("error") == "invalid_grant" or payload.get("message") == "invalid_grant"):
                conn.rollback()
                raise HTTPException(status_code=409, detail={"error": "reauth_required"})
            conn.rollback()
            raise HTTPException(status_code=400, detail={"error": "refresh_failed", "status": status, "detail": payload})

        updated = db_update_tokens_in_tx(cur, seller_id, tok)
        conn.commit()
        return {"seller_id": seller_id, "access_token": updated["access_token"], "expires_at": updated["expires_at"]}

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
