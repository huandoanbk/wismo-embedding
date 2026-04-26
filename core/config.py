from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    vector_db_url: str
    redis_url: str = "redis://localhost:6379/0"
    vastai_redis_url: str = ""  # Redis URL reachable from Vast.ai workers (public IP). Falls back to redis_url.
    s3_endpoint_url: str = ""
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_region: str = "eu-central-1"
    s3_bucket_raw: str = "wismo-raw-files"
    s3_bucket_extracts: str = "wismo-text-extracts"
    internal_shared_secret: str = "changeme"
    vps_callback_url: str = "http://127.0.0.1:8000"
    gpu_docker_image: str = "ghcr.io/wismo/wismo-gpu-worker:latest"
    whisper_gpu_docker_image: str = ""
    embedding_gpu_docker_image: str = ""
    worker_mode: str = "both"
    vastai_api_key: str = ""
    vastai_search_filter: str = (
        "verified=true cuda_max_good>=12.1 gpu_ram>=8 "
        "reliability>0.98 inet_down>=500 num_gpus=1"
    )
    vastai_llm_search_filter: str = (
        "verified=true cuda_max_good>=12.1 gpu_ram>=16 "
        "reliability>0.98 inet_down>=500 num_gpus=1"
    )
    vastai_llm_disk_gb: int = 60
    vastai_max_instances: int = 2
    vastai_max_hourly_cost: float = 0.50
    vastai_llm_max_hourly_cost: float = 0.40
    vastai_health_timeout_seconds: float = 30.0
    vastai_idle_timeout_minutes: int = 30
    gpu_task_request_timeout_seconds: float = 120.0
    worker_alert_throttle_window_minutes: int = 15
    backend_retrieval_base_url: str = ""
    backend_retrieval_search_path: str = "/api/internal/retrieval/test"
    backend_retrieval_auth_token: str = ""
    backend_retrieval_timeout_seconds: float = 10.0
    enrichment_api_base_url: str = ""
    enrichment_api_key: str = ""
    enrichment_model: str = "chunk-enricher"
    enrichment_timeout_seconds: float = 10.0
    dedup_simhash_max_distance: int = 3
    huggingface_hub_token: str = ""
    llm_model_name: str = "Qwen/Qwen3-14B-AWQ"
    llm_quantization: str = "awq"


settings = Settings()
