#!/bin/bash
set -e

QUEUE_ARG="${WORKER_MODE:-both}"
if [ "$QUEUE_ARG" = "both" ]; then
  QUEUE_ARG="whisper,embedding"
elif [ "$QUEUE_ARG" = "llm" ]; then
  # LLM lane uses a dedicated entrypoint that starts vLLM first
  exec "$(dirname "$0")/entrypoint_llm.sh"
fi

echo "=== WISMO GPU Worker | Mode: ${WORKER_MODE:-both} | Queues: ${QUEUE_ARG} ==="
python3 -c "import torch; assert torch.cuda.is_available(), 'CUDA not available'"
python3 /app/gpu_worker/health_server.py &
celery -A gpu_worker.tasks worker --queues="${QUEUE_ARG}" --concurrency=1 --loglevel=info
