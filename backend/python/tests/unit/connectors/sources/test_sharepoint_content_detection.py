"""Regression tests for base-Record lookups that do not include File hashes."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import Connectors, OriginTypes
from app.connectors.sources.microsoft.sharepoint_online.connector import SharePointConnector
from app.models.entities import FileRecord, Record, RecordType


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'stored_hash, current_hash, etag, content_changed, metadata_changed',
    [
        ('same', 'same', 'v1', False, False),
        ('same', 'same', 'v2', False, True),
        ('old', 'new', 'v2', True, True),
        (None, 'hash', 'v1', False, False),
        (None, 'hash', 'v2', True, True),
        ('hash', None, 'v1', False, False),
        ('hash', None, 'v2', True, True),
    ],
)
async def test_drive_content_detection_uses_persisted_file_hash(
    stored_hash, current_hash, etag, content_changed, metadata_changed,
):
    stored_file = FileRecord(
        id='record-1', org_id='org-1', record_name='example.docx',
        record_type=RecordType.FILE, external_record_id='external-1',
        external_revision_id='v1', origin=OriginTypes.CONNECTOR,
        connector_name=Connectors.SHAREPOINT_ONLINE, connector_id='connector-1',
        quick_xor_hash=stored_hash, is_file=True,
    )
    # This is the actual type returned by the provider's external-ID lookup.
    base_record = Record.from_arango_base_record(stored_file.to_arango_base_record())
    assert not hasattr(base_record, 'quick_xor_hash')
    connector = object.__new__(SharePointConnector)
    connector.connector_id = 'connector-1'
    connector.logger = MagicMock()
    connector.data_entities_processor = MagicMock()
    connector.data_entities_processor.get_record_by_external_id = AsyncMock(return_value=base_record)
    connector.data_entities_processor.get_file_record_by_id = AsyncMock(return_value=stored_file)
    connector._pass_drive_date_filters = MagicMock(return_value=True)
    connector._pass_extension_filter = MagicMock(return_value=True)
    connector._create_file_record = AsyncMock(return_value=stored_file)
    connector._get_item_permissions = AsyncMock(return_value=[])
    item = SimpleNamespace(
        id='external-1', name='example.docx', root=None, deleted=None,
        e_tag=etag, file=SimpleNamespace(hashes=SimpleNamespace(quick_xor_hash=current_hash)),
    )

    result = await connector._process_drive_item(item, 'site-1', 'drive-1', [])

    assert result is not None
    assert result.content_changed is content_changed
    assert result.metadata_changed is metadata_changed
    assert result.is_updated is (content_changed or metadata_changed)
    if current_hash is not None:
        connector.data_entities_processor.get_file_record_by_id.assert_awaited_once_with('record-1')
