from functools import lru_cache
from fastapi import Depends, Security
from fastapi.security import SecurityScopes, HTTPAuthorizationCredentials, HTTPBearer
from sqlmodel import Field, Session, SQLModel, create_engine, select, text, func, JSON, Column

from app.auth.json_web_token import JWTPayloadModel, JsonWebToken
from app.auth.custom_exceptions import RequiresAuthenticationException
from app.config import Settings


engine = None

def get_engine():
    if engine:
        return engine
    
    settings = get_settings()

    connection_string = f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}@"\
        f"{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_database}"

    globals()['engine'] = create_engine(connection_string)

    return engine


def get_session():
    with Session(get_engine()) as session:
        yield session


@lru_cache
def get_settings():
    return Settings()


# def get_auth(token: HTTPAuthorizationCredentials = Depends(HTTPBearer())):
def get_auth():
    settings = get_settings()

    # Placeholder token. To be replaced with proper auth
    return JWTPayloadModel(
        sub="default-user",
        org_id="default-org",

        iss="",
        aud=[""],
        iat=0,
        exp=0,
        scope="",
        azp=""
        )
 
    # return JsonWebToken(
    #     jwt_access_token=token.credentials,
    #     jwt_signing_key=settings.jwt_signing_key, 
    #     auth_issuer_url=settings.auth_issuer_url, 
    #     audience=settings.audience,
    #     algorithm=settings.jwt_algorithm
    # ).validate()
