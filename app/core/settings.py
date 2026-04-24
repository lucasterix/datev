from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    database_url: str

    secret_key: str
    jwt_algorithm: str = "HS256"
    access_cookie_name: str = "fz_access_token"

    cors_allowed_origins: str = "https://admin.froehlichdienste.de"

    fzr_api_base_url: str = "http://backend:8000"
    fzr_service_user_email: str | None = None
    fzr_service_user_password: str | None = None

    log_level: str = "INFO"
    log_format: str = "json"

    environment: str = "production"

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_allowed_origins.split(",") if origin.strip()]


settings = Settings()
