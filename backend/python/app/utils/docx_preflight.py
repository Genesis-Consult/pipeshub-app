"""Cheap DOCX inspection used to select a memory-safe parsing route."""

from __future__ import annotations

from io import BytesIO
from pathlib import PurePosixPath
from zipfile import BadZipFile, ZipFile

_LEGACY_VECTOR_EXTENSIONS = {".emf", ".wmf"}
_VML_MARKERS = (
    b"urn:schemas-microsoft-com:vml",
    b"<v:",
    b"<w:pict",
    b"<o:oleobject",
)
_XML_SCAN_CHUNK_SIZE = 64 * 1024
_MAX_XML_MEMBER_SIZE = 64 * 1024 * 1024
_MARKER_OVERLAP = max(len(marker) for marker in _VML_MARKERS) - 1


def _member_contains_vml(archive: ZipFile, member_name: str) -> bool:
    """Stream-scan an OOXML member without inflating it fully in memory."""
    carry = b""
    with archive.open(member_name) as member:
        while chunk := member.read(_XML_SCAN_CHUNK_SIZE):
            window = (carry + chunk).lower()
            if any(marker in window for marker in _VML_MARKERS):
                return True
            carry = window[-_MARKER_OVERLAP:]
    return False


def docx_requires_pdf_fallback(binary: bytes) -> bool:
    """Return whether a DOCX should be converted once to PDF before parsing.

    Docling falls back to a full LibreOffice DOCX->PDF conversion for every
    VML/WMF/EMF object that Pillow cannot decode. A document containing many
    such objects can therefore launch a long series of conversions and retain
    several gigabytes in the indexing process. Detecting those constructs up
    front lets the caller convert the complete document exactly once.

    Invalid ZIP data returns ``False`` so the regular DOCX parser can produce
    its normal, more specific validation error.
    """
    try:
        with ZipFile(BytesIO(binary)) as archive:
            for info in archive.infolist():
                normalized_name = info.filename.lower()
                if PurePosixPath(normalized_name).suffix in _LEGACY_VECTOR_EXTENSIONS:
                    return True

                is_ooxml_control_member = normalized_name.startswith("word/") and (
                    normalized_name.endswith((".xml", ".rels"))
                )
                if not is_ooxml_control_member:
                    continue

                # Very large Word XML is itself a poor fit for the in-process
                # parser. Route it through the same bounded PDF fallback.
                if info.file_size > _MAX_XML_MEMBER_SIZE:
                    return True
                if _member_contains_vml(archive, info.filename):
                    return True
    except (BadZipFile, OSError):
        return False

    return False
