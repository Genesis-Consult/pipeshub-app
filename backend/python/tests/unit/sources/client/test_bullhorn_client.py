import base64
from unittest.mock import AsyncMock

import pytest

from app.sources.client.bullhorn.bullhorn import (
    BullhornApiError,
    BullhornCandidate,
    BullhornClient,
    BullhornCredentials,
)


@pytest.fixture
def client() -> BullhornClient:
    return BullhornClient(
        BullhornCredentials("client", "secret", "user", "password"),
        AsyncMock(),
    )


def candidate(*, parsed_resume_file_id: int | None = None) -> BullhornCandidate:
    return BullhornCandidate(
        id=42,
        name="Alice Example",
        occupation="Java Engineer",
        status="Active",
        email="alice@example.test",
        location="Brussels",
        description="",
        date_last_modified=1_720_000_000_000,
        parsed_resume_file_id=parsed_resume_file_id,
        parsed_resume_metadata=(
            {
                "id": parsed_resume_file_id,
                "name": "alice.pdf",
                "fileExtension": "pdf",
                "contentType": "application/pdf",
                "dateAdded": 10,
            }
            if parsed_resume_file_id is not None
            else None
        ),
        file_attachments=(),
        file_attachment_count=0,
        file_attachments_loaded=parsed_resume_file_id is not None,
        is_deleted=False,
    )


@pytest.mark.asyncio
async def test_resolve_resume_uses_parsed_resume_when_it_is_the_only_cv(
    client: BullhornClient,
) -> None:
    client._list_attachments = AsyncMock()  # type: ignore[method-assign]

    resume = await client.resolve_resume(candidate(parsed_resume_file_id=11))

    assert resume is not None
    assert resume.file_id == 11
    assert resume.extension == "pdf"
    client._list_attachments.assert_not_awaited()


@pytest.mark.asyncio
async def test_resolve_resume_prefers_latest_genesis_cv_case_insensitively(
    client: BullhornClient,
) -> None:
    client._list_attachments = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {
                "id": 10,
                "name": "GC Alice old.pdf",
                "fileExtension": "pdf",
                "dateAdded": 100,
            },
            {
                "id": 11,
                "name": "gc Alice latest.docx",
                "fileExtension": "docx",
                "dateAdded": 200,
            },
            {
                "id": 12,
                "name": "CV Alice newer.pdf",
                "fileExtension": "pdf",
                "dateAdded": 300,
            },
        ]
    )

    resume = await client.resolve_resume(candidate())

    assert resume is not None
    assert resume.file_id == 11


@pytest.mark.asyncio
async def test_resolve_resume_uses_latest_word_or_pdf_without_genesis(
    client: BullhornClient,
) -> None:
    client._list_attachments = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {"id": 20, "name": "Alice.pdf", "dateAdded": 100},
            {"id": 21, "name": "Alice.docx", "dateAdded": 300},
            {"id": 22, "name": "notes.txt", "dateAdded": 400},
        ]
    )

    resume = await client.resolve_resume(candidate())

    assert resume is not None
    assert resume.file_id == 21


@pytest.mark.asyncio
async def test_resolve_resume_returns_none_without_pdf_or_word(
    client: BullhornClient,
) -> None:
    client._list_attachments = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {"id": 30, "name": "GC notes.txt", "dateAdded": 500},
            {"id": 31, "name": "archive.rtf", "dateAdded": 600},
        ]
    )

    assert await client.resolve_resume(candidate()) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('modified_since_ms', 'expected_query'),
    [
        (None, 'isDeleted:0'),
        (1234, 'dateLastModified:[19700101000001 TO *]'),
        (1789134900217, 'dateLastModified:[20260911135500 TO *]'),
        (1709251199999, 'dateLastModified:[20240229235959 TO *]'),
    ],
)
async def test_incremental_candidate_scan_uses_supported_search_api(
    client: BullhornClient,
    modified_since_ms: int | None,
    expected_query: str,
) -> None:
    client._authorized_json = AsyncMock(  # type: ignore[method-assign]
        return_value={"data": []}
    )

    candidates = [item async for item in client.iter_candidates(modified_since_ms)]

    assert candidates == []
    client._authorized_json.assert_awaited_once_with(
        "GET",
        "search/Candidate",
        params={
            "query": expected_query,
            "fields": client.CANDIDATE_FIELDS,
            "count": client.page_size,
            "start": 0,
            "sort": "dateLastModified,id",
        },
    )


@pytest.mark.asyncio
async def test_download_resume_accepts_line_wrapped_base64(
    client: BullhornClient,
) -> None:
    content = b"%PDF-1.7\n" + (b"resume content" * 100)
    encoded = base64.b64encode(content).decode("ascii")
    line_wrapped = "\n".join(
        encoded[index : index + 76] for index in range(0, len(encoded), 76)
    )
    client._authorized_json = AsyncMock(  # type: ignore[method-assign]
        return_value={"File": {"fileContent": line_wrapped}}
    )

    result = await client.download_resume(42, 99)

    assert result == content
    client._authorized_json.assert_awaited_once_with(
        "GET", "file/Candidate/42/99"
    )


@pytest.mark.asyncio
async def test_download_resume_still_rejects_invalid_base64(
    client: BullhornClient,
) -> None:
    client._authorized_json = AsyncMock(  # type: ignore[method-assign]
        return_value={"File": {"fileContent": "not valid base64!"}}
    )

    with pytest.raises(BullhornApiError, match="invalid base64 file content"):
        await client.download_resume(42, 99)


def test_map_candidate_keeps_stable_source_identifiers() -> None:
    mapped = BullhornClient._map_candidate(
        {
            "id": 42,
            "firstName": "Alice",
            "lastName": "Example",
            "dateLastModified": 1234,
            "parsedResumeFile": {"id": 99},
            "address": {"city": "Brussels", "state": "Brussels"},
        }
    )

    assert mapped is not None
    assert mapped.id == 42
    assert mapped.parsed_resume_file_id == 99
    assert mapped.parsed_resume_metadata == {"id": 99}
    assert mapped.file_attachments == ()
    assert not mapped.file_attachments_loaded
    assert mapped.location == "Brussels, Brussels"


def test_quota_snapshot_reads_bullhorn_metering_headers() -> None:
    remaining, reset = BullhornClient._quota_snapshot(
        {
            "RateLimit-Remaining": "2999",
            "RateLimit-Reset": "50",
            "X-RateLimit-Limit-Minute": "3000",
        }
    )

    assert remaining == 2999
    assert reset == 50.0


def test_retry_after_uses_reset_window_with_bounded_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("random.uniform", lambda _start, _end: 0.5)

    delay = BullhornClient._retry_after({"RateLimit-Reset": "50"}, 1)

    assert delay == 50.5


def test_retry_after_caps_untrusted_server_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("random.uniform", lambda _start, _end: 0.0)

    delay = BullhornClient._retry_after({"Retry-After": "3600"}, 1)

    assert delay == BullhornClient.MAX_RATE_LIMIT_WAIT_SECONDS
