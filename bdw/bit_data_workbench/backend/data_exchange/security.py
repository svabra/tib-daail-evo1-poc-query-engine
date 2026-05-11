from __future__ import annotations

import base64
import hashlib
import hmac
import os


PASSWORD_HASH_SCHEME = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 260_000


def hash_password(password: str, *, salt: bytes | None = None) -> str:
    normalized = str(password or "")
    if not normalized:
        raise ValueError("A password is required.")
    salt = salt or os.urandom(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        normalized.encode("utf-8"),
        salt,
        PASSWORD_HASH_ITERATIONS,
    )
    return "$".join(
        (
            PASSWORD_HASH_SCHEME,
            str(PASSWORD_HASH_ITERATIONS),
            base64.urlsafe_b64encode(salt).decode("ascii"),
            base64.urlsafe_b64encode(digest).decode("ascii"),
        )
    )


def verify_password(password: str, password_hash: str) -> bool:
    raw_password = str(password or "")
    raw_hash = str(password_hash or "")
    parts = raw_hash.split("$")
    if len(parts) != 4 or parts[0] != PASSWORD_HASH_SCHEME:
        return False
    try:
        iterations = int(parts[1])
        salt = base64.urlsafe_b64decode(parts[2].encode("ascii"))
        expected_digest = base64.urlsafe_b64decode(parts[3].encode("ascii"))
    except Exception:
        return False
    actual_digest = hashlib.pbkdf2_hmac(
        "sha256",
        raw_password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(actual_digest, expected_digest)
