#!/bin/bash
set -e

cd /app

export WORKER_MODE="${WORKER_MODE:-embedding}"

echo "=== WISMO GPU Embedding Worker | Queue: embedding ==="
python3 -c "import torch; assert torch.cuda.is_available(), 'CUDA not available — torch may be CPU-only build'"
python3 -m gpu_worker.embedding_runtime.health_server &
celery -A gpu_worker.embedding_runtime.tasks worker --queues="embedding" --concurrency=1 --loglevel=info
