import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    host: str = ""
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""

    def __post_init__(self) -> None:
        self.host = os.getenv("PG_HOST", "localhost")
        self.port = int(os.getenv("PG_PORT", "5432"))
        self.database = os.getenv("PG_DB", "benchmark")
        self.user = os.getenv("PG_USER", "postgres")
        self.password = os.getenv("PG_PASSWORD", "postgres")

    # ------------------------------------------------------------------ #
    # Connection string flavours for each driver
    # ------------------------------------------------------------------ #

    @property
    def sqlalchemy_psycopg2_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def sqlalchemy_psycopg3_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def connectorx_url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def asyncpg_dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def psycopg2_dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )

    @property
    def psycopg3_conninfo(self) -> str:
        return self.psycopg2_dsn  # same format


cfg = Config()
