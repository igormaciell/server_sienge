from dotenv import load_dotenv
import os

load_dotenv()

class Settings:
    APP_NAME: str = os.getenv("APP_NAME", "SIENGE BI API")
    APP_ENV: str = os.getenv("APP_ENV", "dev")
    APP_PORT: int = int(os.getenv("APP_PORT", "8000"))

    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "sienge_bi")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")

    DATABASE_URL_ENV: str | None = os.getenv("DATABASE_URL")
    DATABASE_URL_DIRECT: str | None = os.getenv("DATABASE_URL_DIRECT")

    SIENGE_REST_BASE_URL: str = os.getenv("SIENGE_REST_BASE_URL", "").rstrip("/")
    SIENGE_USERNAME: str = os.getenv("SIENGE_USERNAME", "")
    SIENGE_PASSWORD: str = os.getenv("SIENGE_PASSWORD", "")

    @property
    def DATABASE_URL(self) -> str:
        if self.DATABASE_URL_ENV:
            return self.DATABASE_URL_ENV

        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

settings = Settings()