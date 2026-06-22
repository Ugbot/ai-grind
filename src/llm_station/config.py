from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://localhost/llm_station"
    secret_key: str = "change-me-in-production"
    github_client_id: str = ""
    github_client_secret: str = ""
    google_client_id: str = ""
    google_client_secret: str = ""

    model_config = {"env_prefix": "LLM_STATION_", "env_file": ".env"}


settings = Settings()
