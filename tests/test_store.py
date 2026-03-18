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


@pytest.mark.asyncio
async def test_lover_targets_and_daily_counts(temp_db):
    store = Store(temp_db)
    try:
        await store.lover_target_upsert("Alice", "#test", enabled=True, created_by="admin")
        await store.lover_target_upsert("Bob", "#test", enabled=True, created_by="admin")

        rows = await store.lover_targets_list(channel="#test", enabled_only=True)
        assert ("Alice", "#test") in rows
        assert ("Bob", "#test") in rows

        await store.lover_target_set_enabled("Bob", "#test", False)
        rows_enabled = await store.lover_targets_list(channel="#test", enabled_only=True)
        assert ("Alice", "#test") in rows_enabled
        assert ("Bob", "#test") not in rows_enabled

        day = "2026-03-18"
        assert await store.lover_daily_count_get("Alice", day) == 0
        await store.lover_daily_count_increment("Alice", day)
        await store.lover_daily_count_increment("Alice", day)
        assert await store.lover_daily_count_get("Alice", day) == 2
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_lover_enablement_min_max_and_cooldown(temp_db):
    store = Store(temp_db)
    try:
        await store.lover_enablement_set("#test", True, updated_by="admin")
        assert await store.lover_enablement_is_enabled("#test") is True
        enabled = await store.lover_enablement_list_enabled_channels()
        assert "#test" in enabled

        await store.lover_set_min_max(minimum=3, maximum=7)
        mi, ma = await store.lover_get_min_max()
        assert (mi, ma) == (3, 7)

        assert await store.lover_public_cooldown_ready("#test", 1800) is True
        await store.lover_public_cooldown_mark_now("#test")
        assert await store.lover_public_cooldown_ready("#test", 1800) is False
    finally:
        await store.close()
