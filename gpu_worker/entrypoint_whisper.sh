#!/bin/bash
set -e

cd /app
export WORKER_MODE="${WORKER_MODE:-whisper}"

echo "=== WISMO GPU Whisper Worker | Queue: whisper ==="
python3 -c "import ctranslate2; assert ctranslate2.get_cuda_device_count() > 0, 'CUDA not available'"
python3 -m gpu_worker.whisper_runtime.health_server &
celery -A gpu_worker.whisper_runtime.tasks worker --queues="whisper" --concurrency=1 --loglevel=info
