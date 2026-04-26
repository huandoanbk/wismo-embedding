"""CLI entrypoint for embedding service admin and ops tasks."""

from __future__ import annotations

import argparse
import json

from core.database import SessionLocal
from services.dashboard_ops import (
    activate_worker_image_record,
    create_worker_image_record,
    dispatch_audio_jobs,
    get_overview_metrics,
    list_worker_image_rows,
    process_embedding_queue,
    process_text_queue,
    scan_all_enabled_source_configs,
    trigger_source_config_scan,
    validate_worker_image_record,
)
from services.test_search import run_test_search
from tasks import app as embedding_celery_app, vastai_app


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(description="WISMO Embedding Service CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan = subparsers.add_parser("scan", help="Run source discovery scan")
    scan.add_argument("--config", dest="config_id", help="Specific source config id")

    process = subparsers.add_parser("process", help="Process queue items")
    process.add_argument("--type", required=True, choices=["text", "audio", "embedding", "llm"])
    process.add_argument("--vastai", action="store_true", help="Dispatch GPU work through Vast.ai")
    process.add_argument("--batch-size", type=int, default=20)

    status = subparsers.add_parser("status", help="Show embedding service status summary")
    status.add_argument("--json", action="store_true", help="Print raw JSON")

    test_search = subparsers.add_parser("test-search", help="Run backend retrieval integration query")
    test_search.add_argument("query")
    test_search.add_argument("--top-k", type=int, default=3)

    worker_images = subparsers.add_parser("worker-images", help="Manage GPU worker image records")
    worker_image_subparsers = worker_images.add_subparsers(dest="worker_images_command", required=True)

    worker_image_subparsers.add_parser("list", help="List worker image records")

    add_worker_image = worker_image_subparsers.add_parser("add", help="Add one worker image record")
    add_worker_image.add_argument("--lane", required=True, choices=["whisper", "embedding", "llm"])
    add_worker_image.add_argument("--image", required=True, dest="image_ref")
    add_worker_image.add_argument("--display-name")
    add_worker_image.add_argument("--notes")

    validate_worker_image = worker_image_subparsers.add_parser("validate", help="Validate one worker image record")
    validate_worker_image.add_argument("worker_image_id")

    activate_worker_image = worker_image_subparsers.add_parser("activate", help="Activate one worker image record")
    activate_worker_image.add_argument("worker_image_id")

    return parser


def run_cli(argv: list[str] | None = None) -> int:
    """Execute the embedding CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)
    session = SessionLocal()
    try:
        if args.command == "scan":
            if args.config_id:
                payload = trigger_source_config_scan(session, args.config_id)
            else:
                payload = scan_all_enabled_source_configs(session)
            session.commit()
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        if args.command == "process":
            if args.type == "text":
                processed = process_text_queue(batch_size=args.batch_size)
                print(json.dumps({"type": "text", "processed": processed}, ensure_ascii=False, indent=2))
                return 0
            if args.type == "embedding":
                if not args.vastai:
                    parser.error("--vastai is required for --type=embedding")
                payload = process_embedding_queue(
                    session,
                    batch_size=args.batch_size,
                    celery_app=vastai_app,
                )
                session.commit()
                print(json.dumps({"type": "embedding", **payload}, ensure_ascii=False, indent=2))
                return 0
            if args.type == "llm":
                from services.dashboard_ops import dispatch_llm_jobs
                from services.llm_api_client import list_llm_operation_configs

                operation_configs = list_llm_operation_configs(session)
                if any(config.provider == "vastai" for config in operation_configs.values()) and not args.vastai:
                    parser.error("--vastai is required for --type=llm when provider is vastai")
                payload = dispatch_llm_jobs(
                    session,
                    batch_size=args.batch_size,
                    celery_app=vastai_app,
                    api_celery_app=embedding_celery_app,
                )
                session.commit()
                print(json.dumps({"type": "llm", **payload}, ensure_ascii=False, indent=2))
                return 0
            if not args.vastai:
                parser.error("--vastai is required for --type=audio")
            payload = dispatch_audio_jobs(
                session,
                batch_size=args.batch_size,
                celery_app=vastai_app,
            )
            session.commit()
            print(json.dumps({"type": "audio", **payload}, ensure_ascii=False, indent=2))
            return 0

        if args.command == "status":
            payload = get_overview_metrics(session)
            if args.json:
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                for key, value in payload.items():
                    print(f"{key}: {value}")
            return 0


        if args.command == "test-search":
            payload = run_test_search(args.query, top_k=args.top_k)
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        if args.command == "worker-images":
            if args.worker_images_command == "list":
                payload = list_worker_image_rows(session)
                print(json.dumps(payload, ensure_ascii=False, indent=2))
                return 0
            if args.worker_images_command == "add":
                record = create_worker_image_record(
                    session,
                    lane=args.lane,
                    image_ref=args.image_ref,
                    display_name=args.display_name,
                    notes=args.notes,
                )
                session.commit()
                print(
                    json.dumps(
                        {
                            "id": str(record.id),
                            "lane": record.lane,
                            "image_ref": record.image_ref,
                            "validation_status": record.validation_status,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                return 0
            if args.worker_images_command == "validate":
                record = validate_worker_image_record(session, args.worker_image_id)
                session.commit()
                print(
                    json.dumps(
                        {
                            "id": str(record.id),
                            "validation_status": record.validation_status,
                            "validation_error": record.validation_error,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                return 0
            if args.worker_images_command == "activate":
                record = activate_worker_image_record(session, args.worker_image_id)
                session.commit()
                print(
                    json.dumps(
                        {
                            "id": str(record.id),
                            "lane": record.lane,
                            "is_active": record.is_active,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                return 0

        parser.error(f"Unsupported command: {args.command}")
    finally:
        session.close()
    return 1


def main() -> int:
    """Entrypoint used by `python cli.py`."""
    return run_cli()


if __name__ == "__main__":
    raise SystemExit(main())
