import uuid
from dataclasses import dataclass

import jwt
from sqlmodel import SQLModel

from app.auth.custom_exceptions import BadCredentialsException, UnableCredentialsException

class JWTPayloadModel(SQLModel):
    iss: str
    sub: str
    aud: list[str]
    iat: int
    exp: int
    scope: str
    org_id: str
    azp: str

    def sub_uuid(self):
        return uuid.uuid5(uuid.NAMESPACE_URL, self.sub)

    def org_uuid(self):
        return uuid.uuid5(uuid.NAMESPACE_URL, self.org_id)



@dataclass
class JsonWebToken:
    """Perform JSON Web Token (JWT) validation using PyJWT"""

    jwt_access_token: str
    jwt_signing_key: str
    auth_issuer_url: str
    audience: str
    algorithm: str

    def validate(self):
        options = {"require": ["iss", "sub", "aud", "iat", "exp", "scope", "org_id", "azp"]}
        try:
            if self.jwt_signing_key == None:
                jwks_uri = f"{self.auth_issuer_url}.well-known/jwks.json"
                jwks_client = jwt.PyJWKClient(jwks_uri)
                self.jwt_signing_key = jwks_client.get_signing_key_from_jwt(
                    self.jwt_access_token
                ).key

            payload = jwt.decode(
                self.jwt_access_token,
                self.jwt_signing_key,
                algorithms=self.algorithm,
                audience=self.audience,
                issuer=self.auth_issuer_url,
                options=options
            )
        except jwt.exceptions.PyJWKClientError:
            raise UnableCredentialsException
        except jwt.exceptions.InvalidTokenError:
            raise BadCredentialsException
        return JWTPayloadModel.model_validate(payload)
