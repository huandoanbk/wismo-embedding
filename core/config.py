from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    vector_db_url: str
    redis_url: str = 'redis://localhost:6379/0'
    s3_endpoint_url: str = ''
    s3_access_key: str = ''
    s3_secret_key: str = ''
    internal_shared_secret: str = 'changeme'

    class Config:
        env_file = '.env'

settings = Settings()
