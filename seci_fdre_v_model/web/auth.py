"""Authentication helpers for local and hosted control-room modes."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Any


AUTH_SESSION_KEY = "auth_user"


@dataclass(frozen=True)
class AuthenticatedUser:
    subject: str
    email: str
    name: str
    user_key: str

    @classmethod
    def from_claims(cls, claims: dict[str, Any]) -> "AuthenticatedUser":
        subject = str(claims.get("sub") or "").strip()
        if not subject:
            raise ValueError("Auth0 profile is missing subject.")
        email = normalize_email(str(claims.get("email") or ""))
        if not email:
            raise ValueError("Auth0 profile is missing email.")
        name = str(claims.get("name") or claims.get("nickname") or email)
        return cls(subject=subject, email=email, name=name, user_key=user_key_for_subject(subject))

    @classmethod
    def from_session(cls, payload: dict[str, Any] | None) -> "AuthenticatedUser | None":
        if not isinstance(payload, dict):
            return None
        subject = str(payload.get("sub") or "").strip()
        email = normalize_email(str(payload.get("email") or ""))
        user_key = str(payload.get("user_key") or "").strip()
        if not subject or not email or not user_key:
            return None
        return cls(
            subject=subject,
            email=email,
            name=str(payload.get("name") or email),
            user_key=user_key,
        )

    def to_session(self) -> dict[str, str]:
        return {
            "sub": self.subject,
            "email": self.email,
            "name": self.name,
            "user_key": self.user_key,
        }


def normalize_email(value: str) -> str:
    return value.strip().lower()


def user_key_for_subject(subject: str) -> str:
    return hashlib.sha256(subject.encode("utf-8")).hexdigest()[:32]


def auth0_configured() -> bool:
    return bool(auth0_domain() and os.environ.get("AUTH0_CLIENT_ID") and os.environ.get("AUTH0_CLIENT_SECRET"))


def auth0_partially_configured() -> bool:
    values = [auth0_domain(), os.environ.get("AUTH0_CLIENT_ID"), os.environ.get("AUTH0_CLIENT_SECRET")]
    return any(values) and not all(values)


def auth0_domain() -> str:
    domain = (os.environ.get("AUTH0_DOMAIN") or "").strip()
    if domain.startswith("https://"):
        domain = domain[len("https://") :]
    return domain.rstrip("/")


def configured_admin_emails() -> set[str]:
    raw = os.environ.get("SECI_FDRE_V_ADMIN_EMAILS") or ""
    return {normalize_email(part) for part in raw.split(",") if normalize_email(part)}


def is_admin(user: AuthenticatedUser | None) -> bool:
    return bool(user and user.email in configured_admin_emails())
