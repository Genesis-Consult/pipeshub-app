"""PipesHub connector that indexes one selected resume per Bullhorn candidate."""

from __future__ import annotations

import asyncio
import time
import uuid
from io import BytesIO
from typing import TYPE_CHECKING

from fastapi import HTTPException

from app.config.constants.arangodb import Connectors, OriginTypes, ProgressStatus
from app.config.constants.http_status_code import HttpStatusCode
from app.connectors.core.base.connector.connector_service import (
    BaseConnector,
    ConnectorInitError,
)
from app.connectors.core.base.data_processor.data_source_entities_processor import (
    DataSourceEntitiesProcessor,
)
from app.connectors.core.base.sync_point.sync_point import (
    SyncDataPointType,
    SyncPoint,
    generate_record_sync_point_key,
)
from app.connectors.core.constants import IconPaths
from app.connectors.core.registry.auth_builder import AuthBuilder, AuthType
from app.connectors.core.registry.connector_builder import (
    AuthField,
    ConnectorBuilder,
    ConnectorScope,
    CustomField,
    DocumentationLink,
    SyncStrategy,
)
from app.connectors.sources.bullhorn.common.apps import BullhornApp
from app.models.entities import FileRecord, Record, RecordType
from app.models.permission import EntityType, Permission, PermissionType
from app.sources.client.bullhorn.bullhorn import (
    BullhornApiError,
    BullhornCandidate,
    BullhornClient,
    BullhornCredentials,
    BullhornResume,
)
from app.utils.streaming import create_stream_record_response

if TYPE_CHECKING:
    from logging import Logger

    from fastapi.responses import StreamingResponse

    from app.config.configuration_service import ConfigurationService
    from app.connectors.core.base.data_store.data_store import DataStoreProvider
    from app.connectors.core.registry.filters import FilterOptionsResponse


@(
    ConnectorBuilder("Bullhorn")
    .in_group("Bullhorn")
    .with_supported_auth_types("API_TOKEN")
    .with_description(
        "Index one selected Bullhorn resume per candidate with full and incremental "
        "synchronization"
    )
    .with_categories(["Recruiting", "Human Resources"])
    .with_scopes([ConnectorScope.TEAM.value])
    .with_auth(
        [
            AuthBuilder.type(AuthType.API_TOKEN).fields(
                [
                    AuthField(
                        name="client_id",
                        display_name="Client ID",
                        description="OAuth client ID issued by Bullhorn",
                        max_length=200,
                    ),
                    AuthField(
                        name="client_secret",
                        display_name="Client Secret",
                        field_type="PASSWORD",
                        description="OAuth client secret issued by Bullhorn",
                        max_length=500,
                        is_secret=True,
                    ),
                    AuthField(
                        name="username",
                        display_name="API Username",
                        description="Dedicated Bullhorn API user",
                        max_length=255,
                    ),
                    AuthField(
                        name="password",
                        display_name="API Password",
                        field_type="PASSWORD",
                        description="Password of the dedicated Bullhorn API user",
                        max_length=500,
                        is_secret=True,
                    ),
                    AuthField(
                        name="data_center",
                        display_name="Bullhorn Data Center",
                        description="Bullhorn data-center code, for example ger",
                        default_value="ger",
                        max_length=30,
                    ),
                    AuthField(
                        name="candidate_ui_base_url",
                        display_name="Candidate UI Base URL",
                        description=(
                            "Optional Bullhorn Staffing URL used for result links, "
                            "for example https://cls70.bullhornstaffing.com/"
                            "BullhornSTAFFING"
                        ),
                        required=False,
                        max_length=2048,
                    ),
                ]
            )
        ]
    )
    .configure(
        lambda builder: (
            builder.with_icon(IconPaths.connector_icon(Connectors.BULLHORN.value))
            .add_documentation_link(
                DocumentationLink(
                    "Bullhorn REST API",
                    "https://bullhorn.github.io/rest-api-docs/",
                    "docs",
                )
            )
            .with_sync_strategies([SyncStrategy.SCHEDULED, SyncStrategy.MANUAL])
            .with_scheduled_config(True, 60)
            .add_sync_custom_field(
                CustomField(
                    name="candidate_concurrency",
                    display_name="Concurrent candidates",
                    field_type="NUMBER",
                    required=False,
                    default_value="8",
                    description=(
                        "Concurrent Bullhorn candidate requests (bounded from 1 to 20)"
                    ),
                )
            )
            .add_sync_custom_field(
                CustomField(
                    name="staff_group_connector_id",
                    display_name="Staff group connector ID",
                    field_type="TEXT",
                    required=False,
                    default_value="0c7c0373-18a1-4b95-96b6-c519dbc6935f",
                    description=(
                        "PipesHub connector ID that directly synchronizes the Entra "
                        "StaffGC membership (the Genesis SharePoint connector)"
                    ),
                )
            )
            .add_sync_custom_field(
                CustomField(
                    name="staff_group_external_id",
                    display_name="Staff group external ID",
                    field_type="TEXT",
                    required=False,
                    default_value="feb62e75-2b5c-49d7-ba81-9f8c1455f764",
                    description=(
                        "Microsoft Entra object ID of the StaffGC group"
                    ),
                )
            )
            .with_sync_support(True)
            .with_agent_support(False)
        )
    )
    .build_decorator()
)
class BullhornConnector(BaseConnector):
    """Team connector whose CVs are restricted to the Entra StaffGC group."""

    SYNC_POINT_KEY = generate_record_sync_point_key("bullhorn", "candidates", "global")
    INCREMENTAL_OVERLAP_MS = 5 * 60 * 1000
    FULL_RECONCILIATION_INTERVAL_MS = 24 * 60 * 60 * 1000
    DEFAULT_STAFF_GROUP_CONNECTOR_ID = "0c7c0373-18a1-4b95-96b6-c519dbc6935f"
    DEFAULT_STAFF_GROUP_EXTERNAL_ID = "feb62e75-2b5c-49d7-ba81-9f8c1455f764"

    def __init__(
        self,
        logger: Logger,
        data_entities_processor: DataSourceEntitiesProcessor,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        scope: str,
        created_by: str,
    ) -> None:
        super().__init__(
            BullhornApp(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )
        self.connector_name = Connectors.BULLHORN
        self.client: BullhornClient | None = None
        self.candidate_ui_base_url = ""
        self.candidate_concurrency = 8
        self.staff_group_connector_id = self.DEFAULT_STAFF_GROUP_CONNECTOR_ID
        self.staff_group_external_id = self.DEFAULT_STAFF_GROUP_EXTERNAL_ID
        self.record_sync_point = SyncPoint(
            connector_id=connector_id,
            org_id=data_entities_processor.org_id,
            sync_data_point_type=SyncDataPointType.RECORDS,
            data_store_provider=data_store_provider,
        )

    async def init(self) -> bool:
        config = await self.config_service.get_config(
            f"/services/connectors/{self.connector_id}/config"
        )
        auth = (config or {}).get("auth", {})
        required = ("client_id", "client_secret", "username", "password")
        missing = [field for field in required if not auth.get(field)]
        if missing:
            raise ConnectorInitError(
                f"Missing Bullhorn credentials: {', '.join(missing)}"
            )

        self.candidate_ui_base_url = str(
            auth.get("candidate_ui_base_url") or ""
        ).rstrip("/")
        sync_config = (config or {}).get("sync", {})
        try:
            self.candidate_concurrency = min(
                max(int(sync_config.get("candidate_concurrency", 8)), 1), 20
            )
        except (TypeError, ValueError):
            self.candidate_concurrency = 8
        self.staff_group_connector_id = str(
            sync_config.get("staff_group_connector_id")
            or self.DEFAULT_STAFF_GROUP_CONNECTOR_ID
        ).strip()
        self.staff_group_external_id = str(
            sync_config.get("staff_group_external_id")
            or self.DEFAULT_STAFF_GROUP_EXTERNAL_ID
        ).strip()
        await self._reconcile_staff_access()
        self.client = BullhornClient(
            BullhornCredentials(
                client_id=auth["client_id"],
                client_secret=auth["client_secret"],
                username=auth["username"],
                password=auth["password"],
                data_center=str(auth.get("data_center") or "ger").strip().lower(),
            ),
            self.logger,
        )
        try:
            return await self.client.test_connection()
        except BullhornApiError as exc:
            await self.client.close()
            self.client = None
            raise ConnectorInitError(f"Bullhorn connection failed: {exc}") from exc

    async def _reconcile_staff_access(self) -> None:
        async with self.data_store_provider.transaction() as tx_store:
            staff_group = await tx_store.get_user_group_by_external_id(
                connector_id=self.staff_group_connector_id,
                external_id=self.staff_group_external_id,
            )
            if staff_group is None:
                raise ConnectorInitError(
                    "The StaffGC access group is not synchronized in PipesHub"
                )
            await tx_store.reconcile_app_access_from_group(
                self.connector_id,
                self.staff_group_connector_id,
                self.staff_group_external_id,
            )

    async def test_connection_and_access(self) -> bool:
        return bool(self.client and await self.client.test_connection())

    async def run_sync(self) -> None:
        # Scheduled/manual syncs and startup all enter through run_sync().
        # An explicit Full Sync clears the checkpoint before calling us, so the
        # same path also handles first syncs and full reconciliation safely.
        await self.run_incremental_sync()

    async def run_incremental_sync(self) -> None:
        state = await self.record_sync_point.read_sync_point(self.SYNC_POINT_KEY)
        now_ms = int(time.time() * 1000)
        last_full_sync = state.get("last_full_sync_ms")
        if last_full_sync is None or (
            now_ms - int(last_full_sync) >= self.FULL_RECONCILIATION_INTERVAL_MS
        ):
            await self._sync(modified_since_ms=None, mode="full_reconciliation")
            return
        last_success = state.get("last_successful_sync_ms")
        cutoff = (
            max(0, int(last_success) - self.INCREMENTAL_OVERLAP_MS)
            if last_success is not None
            else None
        )
        await self._sync(modified_since_ms=cutoff, mode="incremental")

    async def _sync(self, modified_since_ms: int | None, mode: str) -> None:
        if not self.client:
            raise RuntimeError("Bullhorn connector is not initialized")

        await self._reconcile_staff_access()

        job_id = str(uuid.uuid4())
        started_ms = int(time.time() * 1000)
        counters = {"seen": 0, "upserted": 0, "deleted": 0, "without_cv": 0}
        self.logger.info(
            "[BULLHORN_SYNC] job_id=%s status=started mode=%s cutoff_ms=%s",
            job_id,
            mode,
            modified_since_ms,
        )
        try:
            pending_candidates: list[BullhornCandidate] = []
            seen_candidate_ids: set[int] = set()
            async for candidate in self.client.iter_candidates(modified_since_ms):
                counters["seen"] += 1
                seen_candidate_ids.add(candidate.id)
                pending_candidates.append(candidate)
                if len(pending_candidates) >= self.candidate_concurrency:
                    await self._process_candidate_batch(pending_candidates, counters)
                    pending_candidates = []

            if pending_candidates:
                await self._process_candidate_batch(pending_candidates, counters)

            if mode in {"full", "full_reconciliation"}:
                if seen_candidate_ids:
                    counters["deleted"] += await self._delete_missing_candidates(
                        seen_candidate_ids
                    )
                else:
                    self.logger.warning(
                        "[BULLHORN_SYNC] job_id=%s missing-candidate cleanup "
                        "skipped because Bullhorn returned no candidates",
                        job_id,
                    )

            previous_state = await self.record_sync_point.read_sync_point(
                self.SYNC_POINT_KEY
            )
            last_full_sync_ms = previous_state.get("last_full_sync_ms")
            if mode in {"full", "full_reconciliation"}:
                last_full_sync_ms = started_ms
            await self.record_sync_point.update_sync_point(
                self.SYNC_POINT_KEY,
                {
                    "last_successful_sync_ms": started_ms,
                    "last_full_sync_ms": last_full_sync_ms,
                    "last_mode": mode,
                    "last_job_id": job_id,
                },
            )
            duration_ms = int(time.time() * 1000) - started_ms
            self.logger.info(
                "[BULLHORN_SYNC] job_id=%s status=completed mode=%s duration_ms=%s "
                "seen=%s upserted=%s deleted=%s without_cv=%s "
                "quota_remaining=%s quota_reset_seconds=%s",
                job_id,
                mode,
                duration_ms,
                counters["seen"],
                counters["upserted"],
                counters["deleted"],
                counters["without_cv"],
                self.client.last_quota_remaining,
                self.client.last_quota_reset_seconds,
            )
        except Exception as exc:
            duration_ms = int(time.time() * 1000) - started_ms
            self.logger.error(
                "[BULLHORN_SYNC] job_id=%s status=failed mode=%s duration_ms=%s "
                "failure_reason=%s",
                job_id,
                mode,
                duration_ms,
                type(exc).__name__,
                exc_info=True,
            )
            raise

    async def _process_candidate_batch(
        self,
        candidates: list[BullhornCandidate],
        counters: dict[str, int],
    ) -> None:
        results = await asyncio.gather(
            *(self._prepare_candidate(candidate) for candidate in candidates)
        )
        records = [item for result in results for item in result[0]]
        if records:
            for start in range(0, len(records), 100):
                record_batch = records[start : start + 100]
                await self.data_entities_processor.on_new_records(record_batch)
            counters["upserted"] += len(records)
        counters["deleted"] += sum(result[1] for result in results)
        counters["without_cv"] += sum(result[2] for result in results)

    async def _prepare_candidate(
        self, candidate: BullhornCandidate
    ) -> tuple[list[tuple[Record, list[Permission]]], int, int]:
        if candidate.is_deleted:
            deleted = await self._delete_candidate_records(candidate.id)
            return [], deleted, 0

        assert self.client is not None
        resume = await self.client.resolve_resume(candidate)
        if resume is None:
            deleted = await self._delete_candidate_records(candidate.id)
            return [], deleted, 1

        record = self._to_record(candidate, resume)
        return [(record, self._permissions())], 0, 0

    def _to_record(
        self, candidate: BullhornCandidate, resume: BullhornResume
    ) -> FileRecord:
        details = [candidate.name, resume.name]
        if candidate.occupation:
            details.append(candidate.occupation)
        if candidate.location:
            details.append(candidate.location)
        record_name = "CV - " + " - ".join(details)
        path = f"candidate/{candidate.id}/file/{resume.file_id}"
        revision = ":".join(
            str(value)
            for value in (
                candidate.date_last_modified,
                resume.file_id,
                resume.date_added,
                resume.size or 0,
            )
        )
        weburl = (
            f"{self.candidate_ui_base_url}/OpenWindow.cfm?"
            f"Entity=Candidate&id={candidate.id}"
            if self.candidate_ui_base_url
            else None
        )
        return FileRecord(
            org_id=self.data_entities_processor.org_id,
            record_name=record_name,
            record_type=RecordType.FILE,
            external_record_id=self._external_record_id(candidate.id),
            external_revision_id=revision,
            version=0,
            origin=OriginTypes.CONNECTOR,
            connector_name=Connectors.BULLHORN,
            connector_id=self.connector_id,
            source_created_at=resume.date_added or candidate.date_last_modified,
            source_updated_at=candidate.date_last_modified,
            weburl=weburl,
            mime_type=resume.content_type,
            size_in_bytes=resume.size,
            is_file=True,
            extension=resume.extension,
            path=path,
            inherit_permissions=False,
        )

    async def _delete_candidate_records(self, candidate_id: int) -> int:
        existing = await self.data_entities_processor.get_record_by_external_id(
            self.connector_id, self._external_record_id(candidate_id)
        )
        if not existing:
            return 0
        result = await self.data_entities_processor.on_records_deleted_cascade(
            [existing.id], self.connector_id
        )
        return int((result or {}).get("successfully_deleted", 0))

    async def _delete_missing_candidates(self, seen_candidate_ids: set[int]) -> int:
        statuses = [status.value for status in ProgressStatus]
        async with self.data_store_provider.transaction() as tx_store:
            records = await tx_store.get_records_by_status(
                self.data_entities_processor.org_id,
                self.connector_id,
                statuses,
                limit=None,
            )

        missing_candidate_ids: set[int] = set()
        for record in records:
            external_id = record.external_record_id or ""
            parts = external_id.split(":")
            if len(parts) < 2 or parts[0] != "candidate":
                continue
            try:
                candidate_id = int(parts[1])
            except ValueError:
                continue
            if candidate_id not in seen_candidate_ids:
                missing_candidate_ids.add(candidate_id)

        deleted = 0
        for candidate_id in missing_candidate_ids:
            deleted += await self._delete_candidate_records(candidate_id)
        return deleted

    def _permissions(self) -> list[Permission]:
        return [
            Permission(
                entity_type=EntityType.GROUP,
                type=PermissionType.READ,
                external_id=self.staff_group_external_id,
                source_connector_id=self.staff_group_connector_id,
            )
        ]

    @staticmethod
    def _external_record_id(candidate_id: int) -> str:
        return f"candidate:{candidate_id}:resume"

    async def stream_record(
        self,
        record: Record,
        user_id: str | None = None,
        convertTo: str | None = None,
    ) -> StreamingResponse:
        del user_id, convertTo
        if not self.client:
            raise HTTPException(
                status_code=HttpStatusCode.SERVICE_UNAVAILABLE.value,
                detail="Bullhorn connector is not initialized",
            )
        parts = (record.path or "").split("/")
        if len(parts) < 3 or parts[0] != "candidate":
            raise HTTPException(
                status_code=HttpStatusCode.BAD_REQUEST.value,
                detail="Invalid Bullhorn resume path",
            )
        try:
            candidate_id = int(parts[1])
            if parts[2] == "description":
                candidate = await self.client.get_candidate(candidate_id)
                content = (candidate.description or "").encode("utf-8")
            elif len(parts) == 4 and parts[2] == "file":
                content = await self.client.download_resume(candidate_id, int(parts[3]))
            else:
                raise ValueError("unsupported resume path")
        except (ValueError, BullhornApiError) as exc:
            self.logger.error(
                "[BULLHORN_STREAM] candidate_id=%s failure_reason=%s",
                parts[1] if len(parts) > 1 else "unknown",
                type(exc).__name__,
            )
            raise HTTPException(
                status_code=HttpStatusCode.BAD_GATEWAY.value,
                detail="Unable to retrieve the Bullhorn resume",
            ) from exc

        return create_stream_record_response(
            BytesIO(content),
            filename=f"{record.record_name}.{getattr(record, 'extension', 'bin')}",
            mime_type=record.mime_type,
            fallback_filename=(
                f"resume_{candidate_id}.{getattr(record, 'extension', 'bin')}"
            ),
        )

    async def get_signed_url(self, record: Record) -> str | None:
        return record.weburl

    async def cleanup(self) -> None:
        if self.client:
            await self.client.close()
            self.client = None

    async def reindex_records(self, record_results: list[Record]) -> None:
        await self.data_entities_processor.reindex_existing_records(record_results)

    async def handle_webhook_notification(self, notification: dict) -> None:
        raise NotImplementedError("Bullhorn webhooks are not configured")

    async def get_filter_options(
        self,
        filter_key: str,
        page: int = 1,
        limit: int = 20,
        search: str | None = None,
        cursor: str | None = None,
    ) -> FilterOptionsResponse:
        del filter_key, page, limit, search, cursor
        raise NotImplementedError("Bullhorn connector has no dynamic filters")

    @classmethod
    async def create_connector(
        cls,
        logger: Logger,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        scope: str,
        created_by: str,
        data_entities_processor: DataSourceEntitiesProcessor,
        **kwargs: object,
    ) -> BaseConnector:
        return cls(
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )
