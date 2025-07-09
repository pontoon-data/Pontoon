import jwt
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient

from app.main import settings


def create_test_jwt(payload:dict, expires_in_minutes:int = 15):
    expiration = datetime.now(timezone.utc) + timedelta(minutes=expires_in_minutes)
    token_payload = {
        **payload,
        "exp": expiration,
        "iat": datetime.now(timezone.utc)
    }
    return jwt.encode(
        token_payload, 
        settings.jwt_signing_key, 
        algorithm=settings.jwt_algorithm
    )


def AuthTestClient(app):
    """ 
        Provides a test client that includes a valid JWT Bearer token 

        This works by using a symmetric key (HS256) algorithm on the JWT token
        instead of the RSA algorithm used in production with auth0. Otherwise
        the tokens have the same claims as used in production. The sub (user ID)
        and org_id claims are hardcoded test values that can be used for 
        testing purposes.
    """

    jwt_token = create_test_jwt({
        'sub': 'user00000',
        'org_id': 'pontoon',
        'iss': "",
        'aud': [""],
        'scope': '',
        'azp': ''
    })

    client = TestClient(
        app,
        headers = {
            "Authorization": f"Bearer {jwt_token}"
        }
    )  

    return client