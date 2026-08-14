"""Tests for the memory-safe DOCX routing preflight."""

from __future__ import annotations

from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

from app.utils.docx_preflight import docx_requires_pdf_fallback


def _docx(entries: dict[str, bytes]) -> bytes:
    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
    return output.getvalue()


def test_regular_docx_uses_native_docx_pipeline() -> None:
    binary = _docx(
        {
            "word/document.xml": (
                b'<w:document xmlns:w="http://schemas.openxmlformats.org/'
                b'wordprocessingml/2006/main"><w:p><w:t>Hello</w:t></w:p></w:document>'
            ),
        }
    )

    assert docx_requires_pdf_fallback(binary) is False


def test_vml_namespace_routes_document_to_pdf() -> None:
    binary = _docx(
        {
            "word/document.xml": (
                b'<w:document xmlns:w="http://schemas.openxmlformats.org/'
                b'wordprocessingml/2006/main" xmlns:v="urn:schemas-microsoft-com:vml">'
                b'<w:pict><v:shape><v:imagedata r:id="rId1"/></v:shape></w:pict>'
                b"</w:document>"
            ),
        }
    )

    assert docx_requires_pdf_fallback(binary) is True


def test_legacy_vector_media_routes_document_to_pdf() -> None:
    binary = _docx(
        {
            "word/document.xml": b"<w:document/>",
            "word/media/legacy-image.emf": b"fake emf",
        }
    )

    assert docx_requires_pdf_fallback(binary) is True


def test_invalid_zip_is_left_to_regular_parser() -> None:
    assert docx_requires_pdf_fallback(b"not a docx") is False


def test_marker_split_across_scan_chunks_is_detected() -> None:
    prefix = b"x" * (64 * 1024 - 5)
    binary = _docx(
        {
            "word/document.xml": prefix + b"<w:pict><v:shape/></w:pict>",
        }
    )

    assert docx_requires_pdf_fallback(binary) is True
