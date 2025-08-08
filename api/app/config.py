import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_name: str = "Pontoon API"
    env: str

    # DB
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_database: str

    # CORs
    allow_origin: str

    jwt_algorithm: Optional[str] = Field(default='RS256')
    jwt_signing_key: Optional[str] = Field(default=None)

    # skip source/destination checks - always succeed 
    skip_transfers: Optional[bool] = Field(default=False)
    
    # Telemetry
    pontoon_telemetry_disabled: Optional[bool] = Field(default=False)
    posthog_papik: Optional[str] = Field(default=None)
    posthog_host: Optional[str] = Field(default="https://us.i.posthog.com")
    
    # Use this to load settings from an env file when developing locally
    # model_config  = SettingsConfigDict(env_file=f"{os.environ.get('ENV', 'dev')}.env")
