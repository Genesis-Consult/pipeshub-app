import sys
import types
from collections.abc import Callable
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
    staff_role: object | None = object(),
) -> tuple[BullhornConnector, MagicMock]:
    logger = MagicMock()
    data_entities_processor = MagicMock(org_id="org-1")
    tx_store = MagicMock()
    tx_store.get_app_role_by_external_id = AsyncMock(return_value=staff_role)
    tx_store.reconcile_app_access_from_role = AsyncMock()

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
async def test_init_reconciles_visibility_from_staff_role() -> None:
    connector, tx_store = build_connector()

    with patch(
        "app.connectors.sources.bullhorn.connector.BullhornClient"
    ) as client_class:
        client_class.return_value.test_connection = AsyncMock(return_value=True)
        assert await connector.init() is True

    tx_store.reconcile_app_access_from_role.assert_awaited_once_with(
        "bullhorn-app",
        BullhornConnector.DEFAULT_STAFF_ROLE_CONNECTOR_ID,
        BullhornConnector.DEFAULT_STAFF_ROLE_EXTERNAL_ID,
    )


@pytest.mark.asyncio
async def test_init_fails_closed_when_staff_role_is_missing() -> None:
    connector, tx_store = build_connector(staff_role=None)

    with pytest.raises(ConnectorInitError, match="StaffGC access role"):
        await connector.init()

    tx_store.reconcile_app_access_from_role.assert_not_awaited()
