# WISMO Embedding Service — Claude Code Rules

## Project
- FastAPI + SQLAlchemy + Celery + Redis + Streamlit
- Python 3.11
- Standalone service, KHÔNG phải Django app
- Connects to:
  - postgres-vector (port 5433)
  - Contabo S3
  - Vast.ai

## Purpose
File này hướng dẫn Claude Code khi **thực thi một active change đã được OpenSpec tạo và Owner duyệt/chỉnh**.

Claude không tự quyết định roadmap, không tự chia lại ownership giữa workers, và không tự mở rộng scope ngoài change hiện tại.

Claude phải làm theo:
1. `openspec/changes/<change-name>/proposal.md`
2. `openspec/changes/<change-name>/design.md`
3. `openspec/changes/<change-name>/tasks.md`
4. các `Spec References` được liệt kê trong proposal/design

Nếu Owner đã chỉnh proposal/design/tasks sau khi OpenSpec sinh ra, **bản đã chỉnh là source of truth**.

---

## Execution Boundary

### Claude phù hợp cho
- Pipeline orchestration nhiều bước:
  - ingestion queue
  - extract
  - chunk
  - enrich
  - embed
  - store
- Vast.ai lifecycle / auto-scale / health check / instance orchestration
- Adaptive chunking strategy nhiều nhánh
- Dedup / reindex / cutover logic
- Partial failure / retry / dead-letter / recovery flows
- Multi-file integration giữa worker, storage, DB, queue, dashboard callbacks
- Reranker integration hoặc retrieval-adjacent logic có nhiều bước phối hợp
- Các refactor nhiều file nhưng vẫn nằm trong active change đã duyệt
- Ops / incident / failover logic cho embedding service

### Claude không tự nhận
- Scaffold module đơn giản nếu tasks không yêu cầu
- Extractor/module CRUD nhỏ và độc lập
- Dashboard UI đơn giản
- Cleanup/refactor tiện tay ngoài active change
- Tự ý đổi schema, storage policy, worker ownership hoặc contract shape
- Tự ý làm thay các isolated modules phù hợp hơn với Codex

---

## Workflow Rules
- Trước khi code, đọc `proposal.md`, `design.md`, `tasks.md`.
- Chỉ load các spec files được liệt kê trong `Spec References`.
- Không load toàn bộ specs folder nếu change đã chỉ rõ spec refs.
- Code theo checklist trong `tasks.md`.
- Chạy test đúng scope trước khi check xong từng task quan trọng.
- Chỉ sửa các file nằm trong active change đã được duyệt.
- Nếu đang làm mà phát sinh nhu cầu sửa file ngoài scope, dừng và báo để update change/task trước khi tiếp tục.
- Không tự ý gộp nhiều changes vào một lần implement.
- Nếu proposal/tasks lệch với codebase hiện tại, ưu tiên:
  1. spec refs
  2. proposal/design/tasks đã duyệt
  3. code cũ

---

## Spec Reference Rules

### Full spec
- Full spec: `docs/WISMO_Project_Specification_v18.md`
- Chỉ đọc phần cần thiết cho embedding change hiện tại.

### Embedding domain specs
- `ingestion/spec.md`
  - ingestion pipeline
  - text extraction flow
  - adaptive chunking
  - enrichment
  - processing states
- `retrieval/spec.md`
  - hybrid retrieval
  - dense + sparse
  - RRF
  - reranker
  - multilingual retrieval policy
- `knowledge-schema/spec.md`
  - SQL DDL
  - SQLAlchemy models
  - pgvector schema/indexes
  - chunk states
  - versioning
  - dedup fields
- `storage/spec.md`
  - Contabo S3 buckets
  - retention policy
  - naming conventions
  - text extract schema
  - cleanup rules
- `gpu-worker/spec.md`
  - Vast.ai manager
  - Whisper pipeline
  - batch embedding worker
  - callback endpoints
  - failover rules
- `dashboard/spec.md`
  - Streamlit pages
  - admin/test search flows
  - CLI commands
  - operational controls

### Mandatory interpretation rules
- Không tìm DB schema chi tiết trong `ingestion/spec.md` nếu `knowledge-schema/spec.md` đã định nghĩa.
- Không tìm Vast.ai lifecycle trong `ingestion/spec.md` nếu `gpu-worker/spec.md` đã định nghĩa.
- Không tự suy diễn retention/storage policy nếu `storage/spec.md` đã định nghĩa.
- Không tự suy diễn retrieval/rerank behavior từ trí nhớ; đọc `retrieval/spec.md`.

---

## Architecture / Domain Rules

### Service boundary
- Đây là standalone embedding service, KHÔNG phải Django app.
- Không import Django modules.
- Không access `postgres-app`.
- Chỉ access `postgres-vector` và knowledge/content data liên quan.

### Pipeline boundary
- Pipeline nhiều bước phải giữ state transitions rõ ràng:
  - queued
  - processing
  - partial failure / failed
  - completed
  - reindex / deprecated nếu spec yêu cầu
- Không làm “all-or-nothing” nếu spec cho phép partial continuation.
- Khi có lỗi từng chunk/batch, xử lý theo failure policy trong spec thay vì crash toàn pipeline.

### Storage boundary
- Raw files là temporary theo retention policy.
- Text extracts là canonical artifacts lâu dài nếu spec yêu cầu giữ.
- Không tự đổi bucket/key naming conventions.

### Retrieval / reranking boundary
- Embedding model: BGE-M3
- Reranker: `bge-reranker-v2-m3`
- Không tự thay model family, embedding dimensions, hoặc retrieval ordering nếu spec không cho phép.
- Multilingual retrieval rules phải bám spec.

### GPU / worker boundary
- Vast.ai orchestration phải tôn trọng:
  - auto-scale limits
  - cost guardrails
  - health checks
  - callback protocol
  - idle destroy rules
- Không hardcode hành vi trái với autoscale/failover policy trong spec.

---

## Code Standards

### Language & Tooling
- Python 3.11
- PEP 8
- Formatter/lint: `ruff`
- Type hints bắt buộc cho function signatures
- Import order:
  1. stdlib
  2. third-party
  3. local imports

### Naming
- Classes: `PascalCase`
- Functions: `snake_case`
- Variables: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Files/modules: `snake_case`

Tên phải tự giải thích, tránh tên mơ hồ như `x`, `obj`, `tmp`, `data`, `res` nếu có thể đặt rõ nghĩa hơn.

### Docstrings
- Bắt buộc cho class và function public.
- Với service/pipeline class quan trọng, docstring nên nêu:
  - purpose
  - inputs
  - outputs
  - side effects
  - persistence / cleanup assumptions
  - retry/idempotency assumptions nếu có

### Comments
- Chỉ comment để giải thích **tại sao**
- Không comment điều hiển nhiên
- Với pipeline nhiều bước, có thể dùng numbered steps hoặc section separators để giữ readability

### Readability
- Từng bước rõ ràng
- Early return cho invalid/error states
- Không dồn nhiều logic vào một dòng
- Với orchestration/recovery logic, code phải chia phase rõ ràng

---

## Schema / Types / Contracts
- SQLAlchemy models phải bám `knowledge-schema/spec.md`
- Không tự đổi table/column/index shape nếu spec không yêu cầu
- Extract/enrich/embed payloads phải có types/models rõ ràng
- Nếu change đụng schema:
  - update migration/alembic đúng scope
  - update tests liên quan
  - kiểm tra reindex/cutover impact

---

## Error Handling
- Không bare `except`
- Log error + context trước khi raise lại ở error path quan trọng
- Retry logic phải rõ cho:
  - S3/network
  - Vast.ai lifecycle
  - callback/API edges
- Partial failure path phải rõ
- Dead-letter / failed-state marking phải bám spec
- Không swallow exception âm thầm
- Cleanup temp files/resources phải an toàn kể cả khi pipeline fail giữa chừng

---

## Testing Rules
- Framework: `pytest`
- Dùng async tests nếu module/task cần async behavior
- Mock external systems:
  - S3
  - Vast.ai
  - external HTTP/callbacks
- Extractors cần sample files phù hợp
- Chunker cần deterministic tests cho chunk sizes / boundaries / overlap
- Pipeline/recovery changes cần integration tests đúng scope

### Với các change thuộc orchestration / retry / dead-letter / Vast.ai / rerank
- Phải có coverage cho:
  - success path
  - retry path
  - partial failure path
  - final failure state
- Không report done nếu các flow quan trọng này chưa có coverage phù hợp

---

## Definition of Done
Chỉ report done khi:
- tất cả checklist item trong `tasks.md` đã xong
- tests liên quan đã pass
- lint/format sạch
- không sửa ngoài scope
- code bám đúng spec refs
- không vi phạm service/domain boundaries của embedding repo