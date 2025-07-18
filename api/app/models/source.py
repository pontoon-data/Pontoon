import uuid
import json
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Literal
from pydantic import BaseModel
from sqlmodel import SQLModel, Field, Column, JSON, select

from app.models.common import ConnectionInfo, discriminated_union


class MemoryConnectionInfo(ConnectionInfo):
    vendor_type: Literal["memory"]


class PostgresqlPasswordConnectionInfo(ConnectionInfo):
    vendor_type: Literal["postgresql"]
    auth_type: str = "basic"
    host: str
    user: str
    password: str
    port: int
    database: str


class RedshiftPasswordConnectionInfo(ConnectionInfo):
    vendor_type: Literal["redshift"]
    auth_type: str = "basic"
    host: str
    user: str
    password: str
    port: int
    database: str
    

class SnowflakePasswordConnectionInfo(ConnectionInfo):
    vendor_type: Literal["snowflake"]
    auth_type: str = "access_token"
    user: str
    access_token: str
    account: str
    warehouse: str
    database: str


class BigQueryServiceAccountConnectionInfo(ConnectionInfo):
    vendor_type: Literal["bigquery"]
    auth_type: str = "service_account"
    project_id: str
    service_account: str # JSON blob


ConnectionInfoUnion = discriminated_union(
    MemoryConnectionInfo,
    RedshiftPasswordConnectionInfo, 
    SnowflakePasswordConnectionInfo, 
    BigQueryServiceAccountConnectionInfo,
    PostgresqlPasswordConnectionInfo
)


class Source:

    class Exception(Exception): pass

    class State:
        DRAFT = 'DRAFT'
        CREATED = 'CREATED'

    
    class Stream(BaseModel):
        schema_name: str
        stream_name: str
        fields: List[dict]

    
    class MetaData(BaseModel):
        source_id: uuid.UUID
        updated_at: datetime
        streams: List['Source.Stream']

        @staticmethod
        def mock(source_id: uuid.UUID):
            return Source.MetaData.model_validate({
                "source_id": source_id,
                "updated_at": datetime.now(timezone.utc),
                "streams": [{
                    "schema_name": "pontoon", 
                    "stream_name": "leads",
                    "fields": [
                        {"name": "id", "type": "INT"}, 
                        {"name": "customer_id", "type": "INT"},
                        {"name": "last_modified", "type": "TIMESTAMP"}
                    ]
                }]
            }) 

    
    class Base(SQLModel):
        source_name: str
        vendor_type: str
        is_enabled: bool = False


    class Public(Base):
        source_id: uuid.UUID
        state: str
        created_at: datetime
        modified_at: datetime


    class Detail(Base):
        source_id: uuid.UUID
        state: str
        created_at: datetime
        modified_at: datetime
        connection_info: ConnectionInfoUnion
    

    class Create(Base):
        connection_info: ConnectionInfoUnion
    

    class Update(Base):
        source_name: str | None = None
        vendor_type: str | None = None
        state: Literal["DRAFT", "CREATED"] | None = None
        is_enabled: bool | None = None
        connection_info: ConnectionInfoUnion | None = None


    class Model(Base, table=True):
        __tablename__ = "source"
        
        source_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
        organization_id: uuid.UUID = Field(foreign_key="organization.organization_id")
        connection_info: dict = Field(sa_column=Column(JSON))
        state: str
        created_by: uuid.UUID
        modified_by: uuid.UUID
        created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        modified_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


    @staticmethod
    def get(session, source_id:uuid.UUID) -> Model:
        return session.get(Source.Model, source_id)
    
    
    @staticmethod
    def list(session, offset:int, limit:int, organization_id:uuid.UUID = None) -> List[Public]:
        stmt = (
            select(Source.Model)
            # .where(Source.Model.state == Source.State.CREATED)
            .offset(offset)
            .limit(limit)
        )
        
        if organization_id:
            stmt = stmt.where(Source.Model.organization_id == organization_id)

        return session.exec(stmt).all()


    @staticmethod
    def create(session, source:Create, created_by:uuid.UUID, organization_id:uuid.UUID) -> Model:
        
        new = Source.Create.model_validate(source)

        if new.vendor_type != new.connection_info.vendor_type:
            raise Source.Exception('Invalid source: vendor_type values do not match')

        model = Source.Model()  
        model.source_name = new.source_name
        model.vendor_type = new.vendor_type
        model.is_enabled = new.is_enabled
        model.state = Source.State.DRAFT
        model.created_by = created_by
        model.modified_by = created_by
        model.organization_id = organization_id
        model.connection_info = json.loads(
            new.connection_info.model_dump_json(context={'hide_sensitive': False})
        )
        
        
        session.add(model)
        session.commit()
        session.refresh(model)
        return model


    @staticmethod
    def update(session, source_id:uuid.UUID, source:Update, modified_by:uuid.UUID) -> Model:
        model = session.get(Source.Model, source_id)
        if not model:
            raise Source.Exception('Source does not exist')

        upd = Source.Update.model_validate(source)

        if upd.source_name != None:
            model.source_name = upd.source_name
        
        if upd.is_enabled != None:
            model.is_enabled = upd.is_enabled

        if upd.state != None:
            model.state = upd.state

        if upd.connection_info != None:
            if upd.connection_info.vendor_type != model.vendor_type:
                raise Source.Exception('Invalid update: vendor_type must match existing')
            model.connection_info = json.loads(
                upd.connection_info.model_dump_json(context={'hide_sensitive': False})
            )

        model.modified_by = modified_by
        model.modified_at = datetime.now(timezone.utc)

        session.add(model)
        session.commit()
        session.refresh(model)
        return model


    @staticmethod
    def clone(session, source_id:uuid.UUID, created_by:uuid.UUID) -> Model:
        src = Source.get(session, source_id)
        if src == None:
            raise Source.Exception('Source to clone does not exist')
        if src.state == Source.State.DRAFT:
            raise Source.Exception('Attempting to clone a DRAFT state Source')

        clone = Source.Model()  
        clone.source_name = src.source_name
        clone.vendor_type = src.vendor_type
        clone.is_enabled = src.is_enabled
        clone.state = Source.State.DRAFT
        clone.created_by = created_by
        clone.modified_by = created_by
        clone.organization_id = src.organization_id
        clone.connection_info = src.connection_info
        
        session.add(clone)
        session.commit()
        session.refresh(clone)
        return clone


    @staticmethod
    def delete(session, source_id:uuid.UUID) -> bool:
        source = session.get(Source.Model, source_id)
        if not source:
            raise Source.Exception('Source does not exist')
        session.delete(source)
        session.commit()
        return True