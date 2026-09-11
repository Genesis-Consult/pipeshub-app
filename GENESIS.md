# Genesis extensions

This fork is based on upstream PipesHub `v0.7.0`.
Genesis extensions include BookStack attachment indexing, the Bullhorn connector
with StaffGC access reconciliation, bounded role-based document navigation,
DOCX conversion safeguards, embedded assistant support, and Internal Search by default.

## BookStack attachments

The connector represents each uploaded attachment as a binary `FileRecord` whose external ID is
`attachment/<id>` and whose parent is the BookStack page record `page/<id>`.

- File bytes are fetched from the authenticated `GET /api/attachments/<id>` endpoint only when the
  PipesHub indexing pipeline streams the record.
- The attachment receives the same resolved permissions and record-group scope as its parent page.
- A permission lookup failure is fail-closed: the file is not added or updated, and an existing record
  is not deleted during that failed reconciliation.
- Link-only attachments are deliberately not fetched. Following arbitrary URLs supplied through
  BookStack would introduce an SSRF boundary and would not preserve source permissions.
- Attachment removals are reconciled on every connector run. Page deletion uses the existing cascade
  deletion path so derived vector content is removed as well.
- Attachment content changes are detected from BookStack's `updated_at` value.

Focused regression tests are in
`backend/python/tests/unit/connectors/sources/test_bookstack_attachments.py`.

## Upstream maintenance

For an upstream upgrade, rebase the Genesis branch onto the selected immutable release tag, run the
focused attachment tests and the upstream BookStack connector tests, then build an immutable image.
Never deploy a moving `main` or `latest` reference.
