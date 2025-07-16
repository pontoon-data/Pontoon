from typing import Union
from typing_extensions import Annotated
from sqlmodel import Field
from pydantic import BaseModel, SerializationInfo, field_serializer



class SensitiveFieldsMixin:

    # add sensitive fields to this list
    @field_serializer( 
        'password', 
        'service_account',
        'access_token',
        'aws_access_key_id',
        'aws_secret_access_key', 
        when_used='json', 
        check_fields=False
    )
    def hide_sensitive_fields(self, value:str, info:SerializationInfo) -> str:
        context = info.context
        if context and context.get('hide_sensitive') == False:
            return value
        return '****'


class ConnectionInfo(BaseModel, SensitiveFieldsMixin):
    vendor_type: str  # discriminator field


def discriminated_union(*classes):
    return Annotated[
        Union[*classes],
        Field(discriminator="vendor_type")
    ]