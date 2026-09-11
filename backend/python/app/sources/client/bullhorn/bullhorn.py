"""Bounded, session-aware client for the Bullhorn REST API."""

from __future__ import annotations

import asyncio
import base64
import binascii
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

import aiohttp

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping
    from logging import Logger


class BullhornApiError(RuntimeError):
    """Raised when Bullhorn cannot satisfy a request after bounded retries."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class BullhornCredentials:
    client_id: str
    client_secret: str
    username: str
    password: str
    data_center: str = "ger"


@dataclass(frozen=True)
class BullhornCandidate:
    id: int
    name: str
    occupation: str | None
    status: str | None
    email: str | None
    location: str | None
    description: str | None
    date_last_modified: int
    parsed_resume_file_id: int | None
    parsed_resume_metadata: dict[str, Any] | None
    file_attachments: tuple[dict[str, Any], ...]
    file_attachment_count: int
    file_attachments_loaded: bool
    is_deleted: bool


@dataclass(frozen=True)
class BullhornResume:
    candidate_id: int
    file_id: int
    name: str
    content_type: str
    extension: str
    date_added: int
    size: int | None


class BullhornClient:
    """Caches one Bullhorn REST session and refreshes it only when required."""

    CANDIDATE_FIELDS = ",".join(
        [
            "id",
            "firstName",
            "lastName",
            "name",
            "occupation",
            "status",
            "email",
            "address(city,state)",
            "description",
            "dateLastModified",
            (
                "parsedResumeFile(id,name,description,externalID,contentType,"
                "contentSubType,fileType,fileExtension,fileSize,dateAdded,isDeleted)"
            ),
            (
                "fileAttachments[100](id,name,description,externalID,contentType,"
                "contentSubType,fileType,fileExtension,fileSize,dateAdded,isDeleted)"
            ),
            "isDeleted",
        ]
    )
    ATTACHMENT_FIELDS = ",".join(
        [
            "id",
            "name",
            "description",
            "externalID",
            "contentType",
            "contentSubType",
            "fileType",
            "fileExtension",
            "fileSize",
            "dateAdded",
            "isDeleted",
        ]
    )
    CV_EXTENSIONS = {"pdf", "doc", "docx"}
    RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}
    MAX_RATE_LIMIT_WAIT_SECONDS = 300.0

    def __init__(
        self,
        credentials: BullhornCredentials,
        logger: Logger,
        *,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        page_size: int = 200,
    ) -> None:
        self.credentials = credentials
        self.logger = logger
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.page_size = page_size
        self._session: aiohttp.ClientSession | None = None
        self._auth_lock = asyncio.Lock()
        self._rate_limit_lock = asyncio.Lock()
        self._next_request_at = 0.0
        self._request_interval_seconds = 0.0
        self.last_quota_remaining: int | None = None
        self.last_quota_reset_seconds: float | None = None
        self._refresh_token: str | None = None
        self._rest_url: str | None = None
        self._rest_token: str | None = None

    @property
    def _auth_base_url(self) -> str:
        return f"https://auth-{self.credentials.data_center}.bullhornstaffing.com/oauth"

    @property
    def _rest_login_url(self) -> str:
        return (
            f"https://rest-{self.credentials.data_center}.bullhornstaffing.com/"
            "rest-services/login"
        )

    async def open(self) -> None:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def authenticate(self, *, force: bool = False) -> None:
        await self.open()
        async with self._auth_lock:
            if self._rest_token and self._rest_url and not force:
                return
            try:
                token_payload = (
                    await self._refresh_access_token()
                    if self._refresh_token and not force
                    else await self._authorize_with_credentials()
                )
            except BullhornApiError:
                if not self._refresh_token or force:
                    raise
                self.logger.warning(
                    "[BULLHORN_AUTH] refresh failed; requesting a new "
                    "authorization code"
                )
                self._refresh_token = None
                token_payload = await self._authorize_with_credentials()

            access_token = token_payload.get("access_token")
            self._refresh_token = token_payload.get("refresh_token")
            if not access_token:
                raise BullhornApiError("Bullhorn token response omitted access_token")

            login_payload = await self._request_json(
                "POST",
                self._rest_login_url,
                params={"version": "*", "access_token": access_token},
                authenticated=False,
            )
            self._rest_token = login_payload.get("BhRestToken")
            self._rest_url = login_payload.get("restUrl")
            if not self._rest_token or not self._rest_url:
                raise BullhornApiError("Bullhorn REST login response is incomplete")
            self._rest_url = self._rest_url.rstrip("/") + "/"

    async def test_connection(self) -> bool:
        await self.authenticate()
        payload = await self._authorized_json(
            "GET",
            "search/Candidate",
            params={
                "query": "isDeleted:0",
                "fields": "id",
                "count": 1,
                "start": 0,
            },
        )
        return isinstance(payload.get("data"), list)

    async def iter_candidates(
        self, modified_since_ms: int | None = None
    ) -> AsyncIterator[BullhornCandidate]:
        start = 0
        # Bullhorn's Lucene date fields use UTC yyyyMMddHHmmss, not the epoch
        # milliseconds returned in entity data. Numeric epoch bounds can match
        # years of unchanged candidates instead of the incremental window.
        query = "isDeleted:0"
        if modified_since_ms is not None:
            cutoff = datetime.fromtimestamp(modified_since_ms // 1000, timezone.utc)
            query = f"dateLastModified:[{cutoff:%Y%m%d%H%M%S} TO *]"
        while True:
            payload = await self._authorized_json(
                "GET",
                "search/Candidate",
                params={
                    "query": query,
                    "fields": self.CANDIDATE_FIELDS,
                    "count": self.page_size,
                    "start": start,
                    "sort": "dateLastModified,id",
                },
            )
            rows = payload.get("data") or []
            for row in rows:
                candidate = self._map_candidate(row)
                if candidate:
                    yield candidate
            if len(rows) < self.page_size:
                break
            start += len(rows)

    async def resolve_resume(
        self, candidate: BullhornCandidate
    ) -> BullhornResume | None:
        attachments = list(candidate.file_attachments)
        if (
            not candidate.file_attachments_loaded
            or candidate.file_attachment_count > len(attachments)
        ):
            attachments = await self._list_attachments(candidate.id)

        attachments_by_id: dict[int, dict[str, Any]] = {}
        for item in attachments:
            file_id = self._as_int(item.get("id"))
            if file_id is not None and not item.get("isDeleted", False):
                attachments_by_id[file_id] = item

        if candidate.parsed_resume_file_id is not None:
            parsed_metadata = dict(candidate.parsed_resume_metadata or {})
            parsed_metadata.setdefault("id", candidate.parsed_resume_file_id)
            parsed_metadata.setdefault("name", "resume.pdf")
            parsed_metadata.setdefault("fileExtension", "pdf")
            existing = attachments_by_id.get(candidate.parsed_resume_file_id, {})
            attachments_by_id[candidate.parsed_resume_file_id] = {
                **parsed_metadata,
                **existing,
            }

        eligible = [
            item
            for item in attachments_by_id.values()
            if self._extension(item) in self.CV_EXTENSIONS
        ]
        if not eligible:
            return None

        genesis = [item for item in eligible if self._is_genesis_cv(item)]
        selected = max(genesis or eligible, key=self._attachment_recency)
        file_id = self._as_int(selected.get("id"))
        if file_id is None:
            return None
        extension = self._extension(selected)
        return BullhornResume(
            candidate_id=candidate.id,
            file_id=file_id,
            name=str(
                selected.get("name") or f"resume-{candidate.id}-{file_id}.{extension}"
            ),
            content_type=self._content_type(selected, extension),
            extension=extension,
            date_added=self._as_int(selected.get("dateAdded")) or 0,
            size=self._as_int(selected.get("fileSize")),
        )

    async def get_candidate(self, candidate_id: int) -> BullhornCandidate:
        payload = await self._authorized_json(
            "GET",
            f"entity/Candidate/{candidate_id}",
            params={"fields": self.CANDIDATE_FIELDS},
        )
        row = payload.get("data")
        candidate = self._map_candidate(row) if isinstance(row, dict) else None
        if candidate is None:
            raise BullhornApiError(f"Candidate {candidate_id} was not found")
        return candidate

    async def download_resume(self, candidate_id: int, file_id: int) -> bytes:
        payload = await self._authorized_json(
            "GET", f"file/Candidate/{candidate_id}/{file_id}"
        )
        file_payload = payload.get("File") or payload.get("file") or {}
        encoded = file_payload.get("fileContent")
        if not isinstance(encoded, str):
            raise BullhornApiError("Bullhorn file response omitted fileContent")
        try:
            # Bullhorn line-wraps large file payloads with LF characters.  Strict
            # validation is still useful for detecting corrupt responses, but it
            # rejects otherwise valid MIME-style Base64 unless the transport
            # whitespace is removed first.
            normalized = "".join(encoded.split())
            return base64.b64decode(normalized, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise BullhornApiError(
                "Bullhorn returned invalid base64 file content"
            ) from exc

    async def _list_attachments(self, candidate_id: int) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        start = 0
        while True:
            payload = await self._authorized_json(
                "GET",
                f"entity/Candidate/{candidate_id}/fileAttachments",
                params={
                    "fields": self.ATTACHMENT_FIELDS,
                    "count": self.page_size,
                    "start": start,
                },
            )
            rows = payload.get("data") or []
            result.extend(rows)
            if len(rows) < self.page_size:
                return result
            start += len(rows)

    async def _authorize_with_credentials(self) -> dict[str, Any]:
        assert self._session is not None
        params = {
            "client_id": self.credentials.client_id,
            "username": self.credentials.username,
            "password": self.credentials.password,
            "response_type": "code",
            "action": "Login",
        }
        try:
            async with self._session.get(
                f"{self._auth_base_url}/authorize",
                params=params,
                allow_redirects=False,
            ) as response:
                location = response.headers.get("Location", "")
                if response.status not in {301, 302, 303, 307, 308}:
                    body = await response.text()
                    raise BullhornApiError(
                        f"Bullhorn authorization failed with HTTP {response.status}: "
                        f"{body[:200]}"
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise BullhornApiError("Bullhorn authorization request failed") from exc

        code = parse_qs(urlparse(location).query).get("code", [None])[0]
        if not code:
            raise BullhornApiError("Bullhorn authorization redirect omitted code")
        return await self._request_json(
            "POST",
            f"{self._auth_base_url}/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": self.credentials.client_id,
                "client_secret": self.credentials.client_secret,
            },
            authenticated=False,
        )

    async def _refresh_access_token(self) -> dict[str, Any]:
        return await self._request_json(
            "POST",
            f"{self._auth_base_url}/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self.credentials.client_id,
                "client_secret": self.credentials.client_secret,
            },
            authenticated=False,
        )

    async def _authorized_json(
        self, method: str, path: str, *, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        await self.authenticate()
        for auth_attempt in range(2):
            assert self._rest_url and self._rest_token
            try:
                return await self._request_json(
                    method,
                    f"{self._rest_url}{path.lstrip('/')}",
                    params=params,
                    headers={"BhRestToken": self._rest_token},
                    authenticated=True,
                )
            except BullhornApiError as exc:
                if auth_attempt == 0 and getattr(exc, "status", None) == 401:
                    self._rest_token = None
                    self._rest_url = None
                    await self.authenticate()
                    continue
                raise
        raise BullhornApiError("Bullhorn authentication retry exhausted")

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        authenticated: bool,
    ) -> dict[str, Any]:
        await self.open()
        assert self._session is not None
        for attempt in range(1, self.max_retries + 1):
            try:
                await self._wait_for_rate_limit()
                async with self._session.request(
                    method, url, params=params, data=data, headers=headers
                ) as response:
                    await self._apply_rate_limit_headers(response.headers)
                    if response.status == 401 and authenticated:
                        raise BullhornApiError(
                            "Bullhorn REST session expired", status=401
                        )
                    if response.status in self.RETRYABLE_STATUSES:
                        if attempt == self.max_retries:
                            raise BullhornApiError(
                                f"Bullhorn HTTP {response.status} after {attempt} "
                                "attempts",
                                status=response.status,
                            )
                        retry_after = self._retry_after(response.headers, attempt)
                        self.logger.warning(
                            "[BULLHORN_HTTP] retryable_status=%s attempt=%s "
                            "delay_seconds=%.2f quota_remaining=%s",
                            response.status,
                            attempt,
                            retry_after,
                            self._quota_snapshot(response.headers)[0],
                        )
                        if response.status == 429:
                            await self._defer_requests(retry_after)
                        else:
                            await asyncio.sleep(retry_after)
                        continue
                    if response.status >= 400:
                        body = await response.text()
                        raise BullhornApiError(
                            f"Bullhorn HTTP {response.status}: {body[:200]}",
                            status=response.status,
                        )
                    payload = await response.json(content_type=None)
                    if not isinstance(payload, dict):
                        raise BullhornApiError(
                            "Bullhorn returned a non-object JSON response"
                        )
                    return payload
            except BullhornApiError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == self.max_retries:
                    raise BullhornApiError(
                        f"Bullhorn request failed after {attempt} attempts"
                    ) from exc
                delay = self._backoff_with_jitter(attempt)
                self.logger.warning(
                    "[BULLHORN_HTTP] transport_retry attempt=%s delay_seconds=%.2f",
                    attempt,
                    delay,
                )
                await asyncio.sleep(delay)
        raise BullhornApiError("Bullhorn request retry exhausted")

    async def _wait_for_rate_limit(self) -> None:
        async with self._rate_limit_lock:
            now = time.monotonic()
            scheduled_at = max(now, self._next_request_at)
            self._next_request_at = scheduled_at + self._request_interval_seconds
            delay = scheduled_at - now
        if delay > 0:
            await asyncio.sleep(delay)

    async def _apply_rate_limit_headers(self, headers: Mapping[str, str]) -> None:
        remaining, reset_seconds = self._quota_snapshot(headers)
        if remaining is None or reset_seconds is None:
            return
        self.last_quota_remaining = remaining
        self.last_quota_reset_seconds = reset_seconds
        bounded_reset = min(max(reset_seconds, 0.0), self.MAX_RATE_LIMIT_WAIT_SECONDS)
        interval = bounded_reset / remaining if remaining > 0 else bounded_reset
        async with self._rate_limit_lock:
            self._request_interval_seconds = interval
            if remaining <= 0:
                self._next_request_at = max(
                    self._next_request_at,
                    time.monotonic() + bounded_reset,
                )

    async def _defer_requests(self, delay: float) -> None:
        async with self._rate_limit_lock:
            self._next_request_at = max(self._next_request_at, time.monotonic() + delay)

    @classmethod
    def _retry_after(cls, headers: Mapping[str, str], attempt: int) -> float:
        raw = cls._header_value(headers, "Retry-After", "RateLimit-Reset")
        try:
            base_delay = min(
                max(float(raw or ""), 1.0), cls.MAX_RATE_LIMIT_WAIT_SECONDS
            )
        except (TypeError, ValueError):
            base_delay = min(2 ** (attempt - 1), 8)
        return base_delay + random.uniform(0.0, min(1.0, base_delay * 0.1))

    @staticmethod
    def _backoff_with_jitter(attempt: int) -> float:
        base_delay = min(2 ** (attempt - 1), 8)
        return base_delay + random.uniform(0.0, min(1.0, base_delay * 0.1))

    @classmethod
    def _quota_snapshot(
        cls, headers: Mapping[str, str]
    ) -> tuple[int | None, float | None]:
        remaining_raw = cls._header_value(
            headers,
            "RateLimit-Remaining",
            "X-RateLimit-Remaining-Minute",
            "X-Request-Quota-Remaining",
        )
        reset_raw = cls._header_value(headers, "RateLimit-Reset")
        try:
            remaining = int(remaining_raw) if remaining_raw is not None else None
        except (TypeError, ValueError):
            remaining = None
        try:
            reset = float(reset_raw) if reset_raw is not None else None
        except (TypeError, ValueError):
            reset = None
        return remaining, reset

    @staticmethod
    def _header_value(headers: Mapping[str, str], *names: str) -> str | None:
        normalized = {str(key).casefold(): value for key, value in headers.items()}
        for name in names:
            value = normalized.get(name.casefold())
            if value is not None:
                return str(value)
        return None

    @staticmethod
    def _is_genesis_cv(item: dict[str, Any]) -> bool:
        return str(item.get("name") or "").strip().casefold().startswith("gc")

    @classmethod
    def _attachment_recency(cls, item: dict[str, Any]) -> tuple[int, int]:
        return (
            cls._as_int(item.get("dateAdded")) or 0,
            cls._as_int(item.get("id")) or 0,
        )

    @staticmethod
    def _extension(item: dict[str, Any]) -> str:
        explicit = str(item.get("fileExtension") or "").strip().lower().lstrip(".")
        if explicit:
            return explicit
        name = str(item.get("name") or "")
        return name.rsplit(".", 1)[-1].lower() if "." in name else ""

    @staticmethod
    def _content_type(item: dict[str, Any], extension: str) -> str:
        content_type = str(item.get("contentType") or "").strip()
        content_subtype = str(item.get("contentSubType") or "").strip()
        if content_type and "/" in content_type:
            return content_type
        if content_type and content_subtype:
            return f"{content_type}/{content_subtype}"
        return {
            "pdf": "application/pdf",
            "doc": "application/msword",
            "docx": (
                "application/vnd.openxmlformats-officedocument."
                "wordprocessingml.document"
            ),
            "odt": "application/vnd.oasis.opendocument.text",
            "rtf": "application/rtf",
            "txt": "text/plain",
        }.get(extension, "application/octet-stream")

    @classmethod
    def _map_candidate(cls, row: dict[str, Any]) -> BullhornCandidate | None:
        candidate_id = cls._as_int(row.get("id"))
        if candidate_id is None:
            return None
        first_name = str(row.get("firstName") or "").strip()
        last_name = str(row.get("lastName") or "").strip()
        name = str(row.get("name") or f"{first_name} {last_name}").strip()
        parsed = row.get("parsedResumeFile") or {}
        attachments_loaded = "fileAttachments" in row
        raw_attachments = row.get("fileAttachments") or {}
        if isinstance(raw_attachments, dict):
            attachments = raw_attachments.get("data") or []
            attachment_count = cls._as_int(raw_attachments.get("total")) or len(
                attachments
            )
        elif isinstance(raw_attachments, list):
            attachments = raw_attachments
            attachment_count = len(attachments)
        else:
            attachments = []
            attachment_count = 0
        address = row.get("address") or {}
        location = ", ".join(
            value for value in (address.get("city"), address.get("state")) if value
        )
        return BullhornCandidate(
            id=candidate_id,
            name=name or f"Candidate {candidate_id}",
            occupation=row.get("occupation"),
            status=row.get("status"),
            email=row.get("email"),
            location=location or None,
            description=row.get("description"),
            date_last_modified=cls._as_int(row.get("dateLastModified")) or 0,
            parsed_resume_file_id=cls._as_int(parsed.get("id")),
            parsed_resume_metadata=dict(parsed) if parsed else None,
            file_attachments=tuple(
                dict(item) for item in attachments if isinstance(item, dict)
            ),
            file_attachment_count=attachment_count,
            file_attachments_loaded=attachments_loaded,
            is_deleted=bool(row.get("isDeleted", False)),
        )

    @staticmethod
    def _as_int(value: object) -> int | None:
        if not isinstance(value, (int, float, str)):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
