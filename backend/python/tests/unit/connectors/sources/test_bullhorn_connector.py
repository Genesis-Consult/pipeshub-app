import sys
import types
from collections.abc import AsyncIterator, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

streaming_module_name = "app.utils.streaming"
registry_module_name = "app.connectors.core.registry.connector_registry"
original_streaming_module = sys.modules.get(streaming_module_name)
original_registry_module = sys.modules.get(registry_module_name)

streaming_module = types.ModuleType(streaming_module_name)
streaming_module.create_stream_record_response = MagicMock()
sys.modules[streaming_module_name] = streaming_module
registry_module = types.ModuleType(registry_module_name)


def passthrough_connector(**_kwargs: object) -> Callable[[type], type]:
    def decorator(connector_class: type) -> type:
        return connector_class

    return decorator


registry_module.Connector = passthrough_connector
sys.modules[registry_module_name] = registry_module

from app.connectors.core.base.connector.connector_service import (  # noqa: E402
    ConnectorInitError,
)
from app.connectors.sources.bullhorn.connector import BullhornConnector  # noqa: E402

if original_streaming_module is None:
    sys.modules.pop(streaming_module_name)
else:
    sys.modules[streaming_module_name] = original_streaming_module
if original_registry_module is None:
    sys.modules.pop(registry_module_name)
else:
    sys.modules[registry_module_name] = original_registry_module


def build_connector(
    staff_group: object | None = object(),
) -> tuple[BullhornConnector, MagicMock]:
    logger = MagicMock()
    data_entities_processor = MagicMock(org_id="org-1")
    tx_store = MagicMock()
    tx_store.get_user_group_by_external_id = AsyncMock(return_value=staff_group)
    tx_store.reconcile_app_access_from_group = AsyncMock()

    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=tx_store)
    transaction.__aexit__ = AsyncMock(return_value=False)
    data_store_provider = MagicMock()
    data_store_provider.transaction.return_value = transaction

    config_service = MagicMock()
    config_service.get_config = AsyncMock(
        return_value={
            "auth": {
                "client_id": "client-id",
                "client_secret": "client-secret",
                "username": "api-user",
                "password": "password",
            }
        }
    )
    connector = BullhornConnector(
        logger,
        data_entities_processor,
        data_store_provider,
        config_service,
        "bullhorn-app",
        "team",
        "creator-id",
    )
    return connector, tx_store


@pytest.mark.asyncio
async def test_init_reconciles_visibility_from_staff_group() -> None:
    connector, tx_store = build_connector()

    with patch(
        "app.connectors.sources.bullhorn.connector.BullhornClient"
    ) as client_class:
        client_class.return_value.test_connection = AsyncMock(return_value=True)
        assert await connector.init() is True

    tx_store.reconcile_app_access_from_group.assert_awaited_once_with(
        "bullhorn-app",
        BullhornConnector.DEFAULT_STAFF_GROUP_CONNECTOR_ID,
        BullhornConnector.DEFAULT_STAFF_GROUP_EXTERNAL_ID,
    )


@pytest.mark.asyncio
async def test_init_fails_closed_when_staff_group_is_missing() -> None:
    connector, tx_store = build_connector(staff_group=None)

    with pytest.raises(ConnectorInitError, match="StaffGC access group"):
        await connector.init()

    tx_store.reconcile_app_access_from_group.assert_not_awaited()


@pytest.mark.asyncio
async def test_each_sync_reconciles_visibility_from_staff_group() -> None:
    connector, tx_store = build_connector()

    async def no_candidates(
        _modified_since_ms: int | None,
    ) -> AsyncIterator[None]:
        if False:
            yield None

    connector.client = MagicMock(
        iter_candidates=no_candidates,
        last_quota_remaining=None,
        last_quota_reset_seconds=None,
    )
    connector.record_sync_point.read_sync_point = AsyncMock(return_value={})
    connector.record_sync_point.update_sync_point = AsyncMock()

    await connector._sync(modified_since_ms=None, mode="incremental")

    tx_store.reconcile_app_access_from_group.assert_awaited_once_with(
        "bullhorn-app",
        BullhornConnector.DEFAULT_STAFF_GROUP_CONNECTOR_ID,
        BullhornConnector.DEFAULT_STAFF_GROUP_EXTERNAL_ID,
    )


def test_record_permissions_use_staff_group() -> None:
    connector, _ = build_connector()

    permission = connector._permissions()[0]

    assert permission.entity_type.value == "GROUP"
    assert (
        permission.external_id == BullhornConnector.DEFAULT_STAFF_GROUP_EXTERNAL_ID
    )
    assert (
        permission.source_connector_id
        == BullhornConnector.DEFAULT_STAFF_GROUP_CONNECTOR_ID
    )


@pytest.mark.asyncio
async def test_factory_preserves_injected_processor_and_org_scope() -> None:
    processor = MagicMock(org_id="org-from-factory")
    connector = await BullhornConnector.create_connector(
        logger=MagicMock(),
        data_store_provider=MagicMock(),
        config_service=MagicMock(),
        connector_id="bullhorn-app",
        scope="team",
        created_by="creator-id",
        data_entities_processor=processor,
        future_factory_option=True,
    )

    assert connector.data_entities_processor is processor
    assert connector.data_entities_processor.org_id == "org-from-factory"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'state, expected_mode, expected_cutoff',
    [
        ({'last_full_sync_ms': 1999999900000, 'last_successful_sync_ms': 1999999990000},
         'incremental', 1999999990000 - BullhornConnector.INCREMENTAL_OVERLAP_MS),
        ({}, 'full_reconciliation', None),
        ({'last_full_sync_ms': 2000000000000 - BullhornConnector.FULL_RECONCILIATION_INTERVAL_MS,
          'last_successful_sync_ms': 1999999990000}, 'full_reconciliation', None),
    ],
)
async def test_sync_entrypoint_respects_checkpoint(state, expected_mode, expected_cutoff):
    """The entrypoint used by startup and scheduled/manual sync must be incremental.

    Full Sync deletes the checkpoint before entering, like a first sync.
    Daily reconciliation is still allowed even after a recent incremental run.
    """
    connector, _ = build_connector()
    connector.record_sync_point.read_sync_point = AsyncMock(return_value=state)
    connector._sync = AsyncMock()
    with patch('app.connectors.sources.bullhorn.connector.time.time', return_value=2000000000):
        await connector.run_sync()
    connector._sync.assert_awaited_once_with(modified_since_ms=expected_cutoff, mode=expected_mode)
