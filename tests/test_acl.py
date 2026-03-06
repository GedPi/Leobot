# Tests for system.acl: principal_from_event, _norm_role, effective_role and is_allowed with mock store.
from __future__ import annotations

import pytest

from system.acl import ROLE_ORDER, principal_from_event
from system.types import Event


def _ev(nick: str = "x", user: str | None = None, host: str | None = None, channel: str | None = "#t"):
    return Event(
        nick=nick,
        user=user,
        host=host,
        target=channel or nick,
        channel=channel,
        text=None,
        is_private=not channel,
        raw="",
        cmd="PRIVMSG",
        params=[],
    )


def test_principal_from_event_user_host():
    ev = _ev(nick="Alice", user="alice", host="host.example.com")
    assert principal_from_event(ev) == "alice@host.example.com"


def test_principal_from_event_nick_fallback():
    ev = _ev(nick="Bob", user=None, host=None)
    assert principal_from_event(ev) == "bob"


def test_role_order():
    assert ROLE_ORDER["guest"] < ROLE_ORDER["user"] < ROLE_ORDER["contributor"] < ROLE_ORDER["admin"]


@pytest.mark.asyncio
async def test_effective_role_guest_without_session_or_db():
    from system.acl import ACL

    class MockStore:
        async def get_acl_session(self, key):
            return None
        async def execute(self, *a, **k):
            pass
        async def fetchone(self, *a, **k):
            return None
        async def acl_count_admins(self):
            return 1
        async def acl_get_identity_role(self, ident):
            return None
        def acl_set_identity_role(self, *a):
            pass

    store = MockStore()
    cfg = {"acl": {"admins": [], "contributors": [], "users": [], "guest_allowed": {"commands": ["help", "commands"]}, "master": ""}}
    acl = ACL(store, cfg)
    ev = _ev(nick="Someone", user="u", host="h")
    role = await acl.effective_role(ev)
    assert role == "guest"


@pytest.mark.asyncio
async def test_effective_role_from_mask():
    from system.acl import ACL

    class MockStore:
        async def get_acl_session(self, key):
            return None
        async def execute(self, *a, **k):
            pass
        async def fetchone(self, *a, **k):
            return None
        async def acl_count_admins(self):
            return 1
        async def acl_get_identity_role(self, ident):
            return None
        def acl_set_identity_role(self, *a):
            pass

    store = MockStore()
    cfg = {"acl": {"admins": [], "contributors": [], "users": ["*!*@example.com"], "guest_allowed": {"commands": ["help"]}, "master": ""}}
    acl = ACL(store, cfg)
    ev = _ev(nick="u", user="u", host="example.com")
    role = await acl.effective_role(ev)
    assert role == "user"
