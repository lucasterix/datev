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
    datev_post_auth_redirect: str = "https://admin.froehlichdienste.de/admin/buchhaltung/mitarbeiter"

    # DATEV tenant context. Required as path parameter on every API call.
    datev_consultant_number: str = ""
    datev_client_number: str = ""
    datev_default_client_id_path: str = ""  # e.g. "1694291-10357"

    # === DATEV local bridge (Tailscale + Python reverse-proxy on LuG-PC) ===
    # Reaches the on-premise DATEVconnect API (lohn-api-v3 a.k.a. Payroll-3.1.4).
    # Empty string = bridge disabled (cloud-only mode for local dev).
    datev_local_bridge_url: str = ""

    # === Patti (https://patti.app) ===
    # Form-login auth, see app/clients/patti_client.py for the protocol.
    patti_base_url: str = "https://patti.app"
    patti_login_email: str = ""
    patti_login_password: str = ""
    patti_timeout_seconds: float = 15.0

    @property
    def datev_scopes_list(self) -> list[str]:
        return [s.strip() for s in self.datev_scopes.split() if s.strip()]

    @property
    def datev_is_sandbox(self) -> bool:
        return self.datev_environment.lower() == "sandbox"

    @property
    def datev_platform_path(self) -> str:
        """'/platform-sandbox' or '/platform' depending on environment."""
        return "/platform-sandbox" if self.datev_is_sandbox else "/platform"

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_allowed_origins.split(",") if origin.strip()]


settings = Settings()
