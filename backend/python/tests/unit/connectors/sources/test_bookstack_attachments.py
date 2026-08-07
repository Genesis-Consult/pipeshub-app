"""Focused tests for BookStack binary attachment synchronization."""

import logging
import sys
import types
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

# The production streaming module imports every configured LLM provider. These connector
# unit tests only need its small response factory, which is patched in the streaming tests.
streaming_stub = types.ModuleType("app.utils.streaming")
streaming_stub.create_stream_record_response = MagicMock()
streaming_stub.invoke_with_structured_output_and_reflection = AsyncMock()
sys.modules.setdefault("app.utils.streaming", streaming_stub)

connector_registry_stub = types.ModuleType("app.connectors.core.registry.connector_registry")


class ConnectorDecoratorStub:
    def __init__(self, **_kwargs) -> None:
        pass

    def __call__(self, connector_class) -> type:
        return connector_class


connector_registry_stub.Connector = ConnectorDecoratorStub
sys.modules.setdefault(
    "app.connectors.core.registry.connector_registry",
    connector_registry_stub,
)

from app.config.constants.arangodb import Connectors, OriginTypes  # noqa: E402
from app.connectors.core.registry.filters import FilterCollection  # noqa: E402
from app.connectors.sources.bookstack.connector import BookStackConnector  # noqa: E402
from app.models.entities import FileRecord, RecordType  # noqa: E402
from app.models.permission import EntityType, Permission, PermissionType  # noqa: E402


def response(success=True, data=None, error=None) -> MagicMock:
    result = MagicMock()
    result.success = success
    result.data = data
    result.error = error
    return result


@pytest.fixture()
def connector() -> BookStackConnector:
    processor = MagicMock()
    processor.org_id = "org-1"
    processor.on_new_records = AsyncMock()
    processor.on_record_metadata_update = AsyncMock()
    processor.on_record_content_update = AsyncMock()
    processor.on_updated_record_permissions = AsyncMock()
    processor.on_records_deleted_cascade = AsyncMock()

    tx = MagicMock()
    tx.get_record_by_external_id = AsyncMock(return_value=None)
    tx.get_records_by_parent = AsyncMock(return_value=[])

    provider = MagicMock()

    @asynccontextmanager
    async def transaction() -> AsyncIterator[MagicMock]:
        yield tx

    provider.transaction = transaction
    config = AsyncMock()

    with patch("app.connectors.sources.bookstack.connector.BookStackApp"):
        instance = BookStackConnector(
            logger=logging.getLogger("test.bookstack.attachments"),
            data_entities_processor=processor,
            data_store_provider=provider,
            config_service=config,
            connector_id="connector-1",
            scope="team",
            created_by="user-1",
        )

    instance.bookstack_base_url = "https://bookstack.example.com/"
    instance.data_source = MagicMock()
    instance.sync_filters = FilterCollection()
    instance.indexing_filters = FilterCollection()
    instance._test_tx = tx
    instance._test_processor = processor
    return instance


def page(page_id=7) -> dict:
    return {
        "id": page_id,
        "name": "Security policy",
        "book_id": 2,
        "chapter_id": 3,
        "created_at": "2026-08-01T10:00:00Z",
        "updated_at": "2026-08-02T10:00:00Z",
    }


def attachment(attachment_id=11, external=False) -> dict:
    return {
        "id": attachment_id,
        "name": "policy.pdf",
        "extension": "pdf",
        "uploaded_to": 7,
        "external": external,
        "created_at": "2026-08-03T10:00:00Z",
        "updated_at": "2026-08-04T10:00:00Z",
    }


def read_permission() -> Permission:
    return Permission(
        entity_type=EntityType.ROLE,
        type=PermissionType.READ,
        external_id="role-1",
    )


@pytest.mark.asyncio
async def test_uploaded_attachment_is_streamed_as_decoded_binary(connector) -> None:
    connector.data_source.get_attachment = AsyncMock(return_value=response(data={
        "id": 11,
        "external": False,
        "content": "JVBERi0xLjQ=",
    }))
    record = MagicMock(
        external_record_id="attachment/11",
        record_name="policy.pdf",
        mime_type="application/pdf",
        id="record-11",
    )

    with patch(
        "app.connectors.sources.bookstack.connector.create_stream_record_response"
    ) as create_response:
        create_response.return_value = MagicMock()
        await connector.stream_record(record)

    content_stream = create_response.call_args.args[0]
    chunks = [chunk async for chunk in content_stream]
    assert chunks == [b"%PDF-1.4"]
    connector.data_source.get_attachment.assert_awaited_once_with(11)


@pytest.mark.asyncio
async def test_external_link_attachment_is_never_downloaded(connector) -> None:
    connector.data_source.get_attachment = AsyncMock(return_value=response(data={
        "id": 11,
        "external": True,
        "content": "https://untrusted.example/file.pdf",
    }))
    record = MagicMock(external_record_id="attachment/11", id="record-11")

    with pytest.raises(HTTPException) as error:
        await connector.stream_record(record)

    assert error.value.status_code == 422


@pytest.mark.asyncio
async def test_attachment_record_inherits_page_scope_and_permissions(connector) -> None:
    permission = read_permission()
    connector.data_source.list_attachments = AsyncMock(return_value=response(data={
        "data": [attachment()],
        "total": 1,
    }))
    connector.data_source.get_content_permissions = AsyncMock(return_value=response(data={
        "fallback_permissions": {"inheriting": False},
    }))
    connector._parse_bookstack_permissions = AsyncMock(return_value=[permission])

    await connector._sync_attachments({7: page()}, {})

    connector._test_processor.on_new_records.assert_awaited_once()
    records = connector._test_processor.on_new_records.call_args.args[0]
    record, permissions = records[0]
    assert record.external_record_id == "attachment/11"
    assert record.parent_external_record_id == "page/7"
    assert record.parent_record_type == RecordType.FILE
    assert record.external_record_group_id == "chapter/3"
    assert record.mime_type == "application/pdf"
    assert record.inherit_permissions is False
    assert permissions == [permission]


@pytest.mark.asyncio
async def test_external_links_are_skipped_and_existing_binary_record_is_removed(connector) -> None:
    stale = MagicMock(id="stale-1", external_record_id="attachment/11")
    connector._test_tx.get_records_by_parent = AsyncMock(return_value=[stale])
    connector.data_source.list_attachments = AsyncMock(return_value=response(data={
        "data": [attachment(external=True)],
        "total": 1,
    }))

    await connector._sync_attachments({7: page()}, {})

    connector._test_processor.on_new_records.assert_not_awaited()
    connector._test_processor.on_records_deleted_cascade.assert_awaited_once_with(
        ["stale-1"],
        "connector-1",
    )


@pytest.mark.asyncio
async def test_permission_read_failure_is_fail_closed_without_deleting_known_record(connector) -> None:
    known = MagicMock(id="known-1", external_record_id="attachment/11")
    connector._test_tx.get_records_by_parent = AsyncMock(return_value=[known])
    connector.data_source.list_attachments = AsyncMock(return_value=response(data={
        "data": [attachment()],
        "total": 1,
    }))
    connector.data_source.get_content_permissions = AsyncMock(
        return_value=response(success=False, error="temporary failure")
    )

    await connector._sync_attachments({7: page()}, {})

    connector._test_processor.on_new_records.assert_not_awaited()
    connector._test_processor.on_records_deleted_cascade.assert_not_awaited()
    connector._test_tx.get_records_by_parent.assert_not_awaited()


@pytest.mark.asyncio
async def test_changed_revision_reindexes_existing_attachment(connector) -> None:
    existing = FileRecord(
        id="record-11",
        org_id="org-1",
        record_name="policy.pdf",
        record_type=RecordType.FILE,
        external_record_id="attachment/11",
        external_revision_id="2026-08-03T10:00:00Z",
        external_record_group_id="chapter/3",
        parent_external_record_id="page/7",
        parent_record_type=RecordType.FILE,
        version=1,
        origin=OriginTypes.CONNECTOR,
        connector_name=Connectors.BOOKSTACK,
        connector_id="connector-1",
        mime_type="application/pdf",
        created_at=1,
        updated_at=1,
        is_file=True,
        extension="pdf",
    )
    connector._test_tx.get_record_by_external_id = AsyncMock(return_value=existing)

    update = await connector._process_bookstack_attachment(
        attachment(),
        page(),
        [read_permission()],
        True,
    )

    assert update.is_new is False
    assert update.content_changed is True
    assert update.record.id == "record-11"
    assert update.record.version == 2


@pytest.mark.asyncio
async def test_deleted_page_cascades_to_attachments_and_vectors(connector) -> None:
    connector._test_tx.get_record_by_external_id = AsyncMock(
        return_value=MagicMock(id="page-record-7")
    )

    await connector._handle_page_delete_event({"detail": "(7) Security policy"})

    connector._test_processor.on_records_deleted_cascade.assert_awaited_once_with(
        ["page-record-7"],
        "connector-1",
    )


@pytest.mark.asyncio
async def test_attachment_listing_failure_never_runs_partial_reconciliation(connector) -> None:
    connector.data_source.list_attachments = AsyncMock(
        return_value=response(success=False, error="rate limited")
    )

    with pytest.raises(RuntimeError, match="rate limited"):
        await connector._sync_attachments({7: page()}, {})

    connector._test_tx.get_records_by_parent.assert_not_awaited()
    connector._test_processor.on_records_deleted_cascade.assert_not_awaited()
