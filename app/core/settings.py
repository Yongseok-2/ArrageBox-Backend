import os

from dotenv import load_dotenv

# Load .env values before reading settings.
load_dotenv()


class Settings:
    port: int = int(os.environ["PORT"])
    kafka_bootstrap_servers: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    email_raw_topic: str = os.environ["EMAIL_RAW_TOPIC"]
    kafka_group_id: str = os.environ["KAFKA_GROUP_ID"]
    postgres_dsn: str = os.environ["POSTGRES_DSN"]
    gemini_api_key: str = os.environ["GEMINI_API_KEY"]
    gemini_model: str = os.environ["GEMINI_MODEL"]
    gemini_timeout_seconds: int = int(os.environ["GEMINI_TIMEOUT_SECONDS"])
    gemini_enabled: bool = os.environ["GEMINI_ENABLED"].lower() == "true"
    gemini_batch_size: int = int(os.environ["GEMINI_BATCH_SIZE"])
    analysis_confidence_threshold: float = float(os.environ["ANALYSIS_CONFIDENCE_THRESHOLD"])
    redis_url: str = os.environ["REDIS_URL"]
    email_body_ttl_seconds: int = int(os.environ["EMAIL_BODY_TTL_SECONDS"])
    auth_access_cookie_name: str = os.environ["AUTH_ACCESS_COOKIE_NAME"]
    auth_refresh_cookie_name: str = os.environ["AUTH_REFRESH_COOKIE_NAME"]
    auth_expires_cookie_name: str = os.environ["AUTH_EXPIRES_COOKIE_NAME"]
    auth_cookie_secure: bool = os.environ["AUTH_COOKIE_SECURE"].lower() == "true"
    auth_cookie_samesite: str = os.environ["AUTH_COOKIE_SAMESITE"]
    auth_cookie_domain: str | None = os.getenv("AUTH_COOKIE_DOMAIN") or None
    auth_access_cookie_max_age: int = int(os.environ["AUTH_ACCESS_COOKIE_MAX_AGE"])
    auth_refresh_cookie_max_age: int = int(os.environ["AUTH_REFRESH_COOKIE_MAX_AGE"])


settings = Settings()
