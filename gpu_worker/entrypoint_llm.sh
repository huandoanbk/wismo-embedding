#!/bin/bash
set -e

cd /app

MODEL_NAME="${LLM_MODEL_NAME:-Qwen/Qwen3-14B-AWQ}"
LLM_QUANTIZATION="${LLM_QUANTIZATION:-awq}"
VLLM_PORT="${VLLM_PORT:-8000}"
TENSOR_PARALLEL="${TENSOR_PARALLEL_SIZE:-1}"
MAX_MODEL_LEN="${VLLM_MAX_MODEL_LEN:-4096}"

echo "=== WISMO LLM Worker | Model: ${MODEL_NAME} | Queue: llm ==="
python3 -c "import torch; assert torch.cuda.is_available(), 'CUDA not available'"

# Start vLLM inference server in background
VLLM_ARGS=(
  --model "${MODEL_NAME}"
  --port "${VLLM_PORT}"
  --tensor-parallel-size "${TENSOR_PARALLEL}"
  --max-model-len "${MAX_MODEL_LEN}"
  --dtype "float16"
  --enforce-eager
)

if [ -n "${LLM_QUANTIZATION}" ]; then
  VLLM_ARGS+=(--quantization "${LLM_QUANTIZATION}")
fi

python3 -m vllm.entrypoints.openai.api_server "${VLLM_ARGS[@]}" &
VLLM_PID=$!

# Wait for vLLM to be ready before starting Celery worker
echo "Waiting for vLLM server to be ready..."
until curl -sf "http://localhost:${VLLM_PORT}/health" > /dev/null 2>&1; do
  sleep 5
  if ! kill -0 "${VLLM_PID}" 2>/dev/null; then
    echo "ERROR: vLLM process died unexpectedly"
    exit 1
  fi
done
echo "vLLM ready on port ${VLLM_PORT}."

# Export runtime env for tasks
export VLLM_BASE_URL="http://localhost:${VLLM_PORT}"
LLM_QUEUE_NAMES="${LLM_QUEUE_NAMES:-llm_refinement,llm_enrichment}"

# Start health server in background
python3 -m gpu_worker.llm_runtime.health_server &

# Start Celery worker consuming the LLM operation queues
celery -A gpu_worker.llm_runtime.tasks worker \
  --queues="${LLM_QUEUE_NAMES}" \
  --concurrency=1 \
  --loglevel=info
