# WISMO Embedding Service — Codex Rules

## Project
- FastAPI + SQLAlchemy + Celery + Redis + Streamlit
- Python 3.11
- Standalone service, KHÔNG phải Django app

## Purpose
File này hướng dẫn Codex khi **thực thi một active change đã được OpenSpec tạo và Owner duyệt/chỉnh**.

Codex không tự quyết định roadmap, không tự chia lại ownership giữa workers, và không tự mở rộng scope ngoài change hiện tại.

Codex phải làm theo:
1. `openspec/changes/<change-name>/proposal.md`
2. `openspec/changes/<change-name>/design.md`
3. `openspec/changes/<change-name>/tasks.md`
4. các `Spec References` được liệt kê trong proposal/design

Nếu Owner đã chỉnh proposal/design/tasks sau khi OpenSpec sinh ra, **bản đã chỉnh là source of truth**.

---

## Execution Boundary

### Codex phù hợp cho
- Project scaffold / config / Docker setup
- SQLAlchemy models / Alembic migrations
- Text extractors:
  - PDF
  - DOCX
  - EPUB
  - TXT
  - PPTX
- S3 connector / storage helpers
- Queue/task modules scope hẹp
- CLI commands
- Streamlit dashboard pages
- Typed service modules độc lập
- Unit tests / API tests / extractor tests / dashboard tests
- Small-to-medium refactors trong một lane rõ ràng

### Codex không tự nhận
- Vast.ai orchestration phức tạp
- End-to-end pipeline recovery nhiều bước
- Multi-file orchestration refactor lớn
- Dedup/cutover/reindex policy refactor rộng
- Cross-module coordination lớn giữa queue, worker, callbacks, dashboard
- Cleanup/refactor ngoài active change

---

## Workflow Rules
- Nhận task → đọc `proposal.md`, `design.md`, `tasks.md`
- Chỉ load đúng các spec files được list trong `Spec References`
- Không load toàn bộ specs folder nếu change đã chỉ rõ spec refs
- Code theo từng checkbox trong `tasks.md`
- Chạy test/lint đúng scope trước khi check item done
- Chỉ sửa các file nằm trong active change đã được duyệt
- Nếu task phát sinh cần đụng file ngoài scope, dừng và báo để update change/task
- Không opportunistic refactor
- Không tự ý sửa orchestration/failover logic lớn nếu change không giao

---

## Spec Reference Rules

### Full spec
- Full spec: `docs/WISMO_Project_Specification_v18.md`
- Chỉ dùng khi change yêu cầu; bình thường ưu tiên domain specs embedding.

### Embedding domain specs
- `ingestion/spec.md`
  - pipeline steps
  - extraction/chunking/enrichment flow
- `retrieval/spec.md`
  - retrieval + rerank logic
  - cache / multilingual policy
- `knowledge-schema/spec.md`
  - schema
  - SQLAlchemy models
  - indexes
  - versioning
  - dedup fields
- `storage/spec.md`
  - S3 connector
  - bucket policies
  - key naming
  - text extract format
- `gpu-worker/spec.md`
  - Vast.ai worker details
  - Whisper/batch-embed worker contracts
  - callbacks
- `dashboard/spec.md`
  - Streamlit pages
  - admin flows
  - CLI commands

### Mandatory interpretation rules
- Schema/model shape đọc từ `knowledge-schema/spec.md`.
- Vast.ai/worker behavior đọc từ `gpu-worker/spec.md`.
- Storage policy đọc từ `storage/spec.md`.
- Retrieval/rerank logic đọc từ `retrieval/spec.md`.
- Không tự suy diễn chunking/retry/state policy nếu `ingestion/spec.md` đã định nghĩa.

---

## Domain / Architecture Rules
- Đây là standalone service, KHÔNG import Django modules.
- Chỉ access `postgres-vector`.
- KHÔNG access user data.
- Không tự thêm dependency lên backend app internals.
- SQLAlchemy models phải match schema spec.
- Nếu change chạm pipeline state, phải giữ transition rõ ràng và testable.
- Nếu change chạm storage keys/buckets, phải bám naming/retention rules trong spec.

---

## UI / Dashboard / Wiring Rules
- Dashboard chỉ là admin/ops surface, không được tự mang business policy không có trong spec.
- CLI commands phải rõ input/output và side effects.
- Nếu wiring callback/API:
  - map request/response rõ ràng
  - không hardcode assumptions ngoài spec
  - log operational context đủ dùng

---

## Code Standards

### Language & Tooling
- Python 3.11
- PEP 8
- Formatter/lint: `ruff`
- Type hints bắt buộc

### Import order
1. stdlib
2. third-party
3. local imports

### Naming
- Classes: `PascalCase`
- Functions: `snake_case`
- Variables: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Files: `snake_case`

Tên phải rõ nghĩa, tránh tên mơ hồ hoặc một ký tự nếu không cần.

### Docstrings
- Bắt buộc cho class và function public
- Với service/module chính, docstring nên nêu:
  - mục đích
  - input/output
  - side effects
  - cleanup/retry assumptions nếu có

### Comments
- Giải thích tại sao, không giải thích điều hiển nhiên
- Có thể dùng numbered steps cho pipeline code nếu giúp readability

### Readability
- Từng bước rõ ràng
- Early return cho edge/error states
- Không dồn logic phức tạp vào một dòng
- Pipeline code phải có biến trung gian rõ nghĩa

---

## Schema / Types
- Models và DB shape phải bám `knowledge-schema/spec.md`
- Không tự đổi dimensions, index strategy, chunk state names, hoặc metadata schema nếu spec không yêu cầu
- Nếu cần model nội bộ riêng cho extractor/dashboard layer, tách rõ khỏi DB model

---

## Error Handling
- Không bare `except`
- Retry cho S3/network operations khi spec yêu cầu
- Log error + context trước khi raise ở error path quan trọng
- Partial failure handling phải rõ
- Không swallow exception âm thầm
- Temp files/resources phải được cleanup an toàn

---

## Testing Rules
- Framework: `pytest`
- Extractor tests dùng sample files phù hợp
- Mock S3 + Vast.ai + external HTTP khi cần
- Unit tests cho isolated modules là bắt buộc
- API / CLI / dashboard changes cần tests đúng scope

### Với extractor / storage / queue / schema changes
- Phải test:
  - success path
  - invalid input / unsupported type
  - error path phù hợp
- Nếu có mapping/parsing logic, phải test mapping đó
- Không report done nếu flow chính chưa có coverage tối thiểu

---

## Definition of Done
Chỉ report done khi:
- tất cả checklist trong `tasks.md` đã xong
- tests liên quan đã pass
- lint/format sạch
- không sửa ngoài scope
- module bám đúng domain spec đã chỉ định
- không lấn sang orchestration/refactor lớn không được giao