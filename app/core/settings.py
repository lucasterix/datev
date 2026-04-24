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

    # === DATEV OAuth / API ===
    datev_environment: str = "sandbox"  # "sandbox" | "production"
    datev_client_id: str = ""
    datev_client_secret: str = ""
    datev_redirect_uri: str = ""
    datev_discovery_url: str = ""
    datev_scopes: str = "openid profile"
    # Where to send the browser after a successful OAuth callback.
    datev_post_auth_redirect: str = "https://admin.froehlichdienste.de/admin/buchhaltung/mitarbeiter"

    @property
    def datev_scopes_list(self) -> list[str]:
        return [s.strip() for s in self.datev_scopes.split() if s.strip()]

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_allowed_origins.split(",") if origin.strip()]


settings = Settings()
