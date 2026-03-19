import os

from dotenv import load_dotenv

# Load .env values before reading settings.
load_dotenv()


class Settings:
    port: int = int(os.getenv("PORT", "8888"))
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    email_raw_topic: str = os.getenv("EMAIL_RAW_TOPIC", "email-raw")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "inboxzero-email-workers")
    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN",
        "postgresql://inboxzero:inboxzero@localhost:5432/inboxzero",
    )
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
    gemini_timeout_seconds: int = int(os.getenv("GEMINI_TIMEOUT_SECONDS", "20"))
    analysis_confidence_threshold: float = float(
        os.getenv("ANALYSIS_CONFIDENCE_THRESHOLD", "0.72")
    )
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    email_body_ttl_seconds: int = int(os.getenv("EMAIL_BODY_TTL_SECONDS", "600"))
    auth_access_cookie_name: str = os.getenv("AUTH_ACCESS_COOKIE_NAME", "g_access_token")
    auth_refresh_cookie_name: str = os.getenv("AUTH_REFRESH_COOKIE_NAME", "g_refresh_token")
    auth_expires_cookie_name: str = os.getenv("AUTH_EXPIRES_COOKIE_NAME", "g_expires_at")
    auth_cookie_secure: bool = os.getenv("AUTH_COOKIE_SECURE", "true").lower() == "true"
    auth_cookie_samesite: str = os.getenv("AUTH_COOKIE_SAMESITE", "none")
    auth_cookie_domain: str | None = os.getenv("AUTH_COOKIE_DOMAIN") or None
    auth_access_cookie_max_age: int = int(os.getenv("AUTH_ACCESS_COOKIE_MAX_AGE", "3600"))
    auth_refresh_cookie_max_age: int = int(
        os.getenv("AUTH_REFRESH_COOKIE_MAX_AGE", str(60 * 60 * 24 * 30))
    )


settings = Settings()
