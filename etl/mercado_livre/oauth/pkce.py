import base64
import hashlib
import secrets
import string


# PKCE (RFC 7636): code_verifier deve ter entre 43 e 128 caracteres
_PKCE_MIN = 43
_PKCE_MAX = 128

# Charset permitido pelo RFC 7636 (unreserved)
_PKCE_ALLOWED = string.ascii_letters + string.digits + "-._~"


def b64url(data: bytes) -> str:
    """
    Base64 URL-safe sem padding, conforme PKCE.
    """
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def make_state(nbytes: int = 16) -> str:
    """
    Gera state anti-CSRF (hex). Padrão: 16 bytes => 32 chars hex (~128 bits).
    """
    if nbytes < 16:
        # 128 bits é um mínimo razoável para state
        nbytes = 16
    return secrets.token_hex(nbytes)


def make_verifier(length: int = 64) -> str:
    """
    Gera code_verifier com charset permitido e tamanho entre 43 e 128.
    Padrão: 64 chars (boa margem).
    """
    if length < _PKCE_MIN:
        length = _PKCE_MIN
    if length > _PKCE_MAX:
        length = _PKCE_MAX

    # Geração criptograficamente segura
    return "".join(secrets.choice(_PKCE_ALLOWED) for _ in range(length))


def make_challenge(verifier: str) -> str:
    """
    Gera code_challenge para método S256.
    """
    validate_verifier(verifier)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return b64url(digest)


def validate_verifier(verifier: str) -> None:
    """
    Valida o code_verifier de acordo com RFC 7636.
    Levanta ValueError se inválido.
    """
    if not isinstance(verifier, str) or verifier.strip() == "":
        raise ValueError("code_verifier vazio")

    if not (_PKCE_MIN <= len(verifier) <= _PKCE_MAX):
        raise ValueError(f"code_verifier tamanho inválido: {len(verifier)} (esperado {_PKCE_MIN}-{_PKCE_MAX})")

    # Charset restrito do PKCE
    for ch in verifier:
        if ch not in _PKCE_ALLOWED:
            raise ValueError("code_verifier contém caractere inválido (fora do charset PKCE)")


def validate_state(state: str) -> None:
    """
    Valida state (anti-CSRF). Aqui o state é hex (token_hex).
    Levanta ValueError se inválido.
    """
    if not isinstance(state, str) or state.strip() == "":
        raise ValueError("state vazio")

    # state em hex: apenas 0-9a-f e tamanho par (token_hex)
    if any(c not in "0123456789abcdef" for c in state.lower()):
        raise ValueError("state inválido (não-hex)")

    # mínimo: 16 bytes => 32 chars
    if len(state) < 32:
        raise ValueError("state muito curto")
