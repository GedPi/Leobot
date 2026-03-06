# Tests for system.store: ACL helpers and acl_policies with a temp SQLite DB.
from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from system.store import Store


@pytest.fixture
def temp_db():
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    path = f.name
    f.close()
    yield path
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass


@pytest.mark.asyncio
async def test_acl_identity_role(temp_db):
    store = Store(temp_db)
    try:
        await store.acl_set_identity_role("alice", "admin")
        role = await store.acl_get_identity_role("alice")
        assert role == "admin"
        await store.acl_set_identity_role("alice", "contributor")
        role = await store.acl_get_identity_role("alice")
        assert role == "contributor"
        await store.acl_del_identity("alice")
        role = await store.acl_get_identity_role("alice")
        assert role is None
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_acl_list_identities(temp_db):
    store = Store(temp_db)
    try:
        await store.acl_set_identity_role("a", "admin")
        await store.acl_set_identity_role("b", "admin")
        await store.acl_set_identity_role("c", "user")
        admins = await store.acl_list_identities("admin")
        assert set(admins) == {"a", "b"}
        users = await store.acl_list_identities("user")
        assert "c" in users
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_acl_command_perms(temp_db):
    store = Store(temp_db)
    try:
        await store.acl_set_command_min_role("weather", "guest")
        r = await store.acl_get_command_min_role("weather")
        assert r == "guest"
        await store.acl_del_command_min_role("weather")
        r = await store.acl_get_command_min_role("weather")
        assert r is None
        perms = await store.acl_list_command_perms()
        assert isinstance(perms, list)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_acl_policies(temp_db):
    store = Store(temp_db)
    try:
        await store.acl_set_policy("#test", "weather", "query", "guest")
        r = await store.acl_get_policy("#test", "weather", "query")
        assert r == "guest"
        await store.acl_del_policy("#test", "weather", "query")
        r = await store.acl_get_policy("#test", "weather", "query")
        assert r is None
        await store.acl_set_policy("#ch", "svc", "cap", "user")
        rows = await store.acl_list_policies("#ch")
        assert len(rows) == 1
        assert rows[0][1] == "svc" and rows[0][3] == "user"
    finally:
        await store.close()
