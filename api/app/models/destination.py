import uuid
import json
from datetime import datetime, timezone
from typing import Optional, List, Literal
from pydantic import BaseModel, field_validator
from sqlmodel import SQLModel, Field, Column, JSON, select

from app.models.recipient import Recipient
from app.models.model import Model
from app.models.transfer import Transfer
from app.models.common import ConnectionInfo, discriminated_union


class ConsoleConnectionInfo(ConnectionInfo):
    vendor_type: Literal["console"]


class RedshiftS3ConnectionInfo(ConnectionInfo):
    vendor_type: Literal["redshift"]
    auth_type: str = "basic"
    host: str
    user: str
    password: str
    port: int
    database: str
    target_schema: str
    s3_bucket: str
    s3_region: str
    s3_prefix: str
    iam_role: str
    aws_access_key_id: str
    aws_secret_access_key: str
    

class SnowflakeSMSConnectionInfo(ConnectionInfo):
    vendor_type: Literal["snowflake"]
    auth_type: str = "access_token"
    stage_name: str = "PONTOON"
    create_stage: bool = True
    delete_stage: bool = True
    user: str
    access_token: str
    account: str
    warehouse: str
    database: str
    target_schema: str


class BigQueryGCSServiceAccountConnectionInfo(ConnectionInfo):
    vendor_type: Literal["bigquery"]
    auth_type: str = "service_account"
    project_id: str
    target_schema: str
    gcs_bucket_name: str
    gcs_bucket_path: str
    service_account: str # JSON blob


class PostgresConnectionInfo(ConnectionInfo):
    vendor_type: Literal["postgresql"]
    auth_type: str = "basic"
    host: str
    user: str
    password: str
    port: int
    database: str
    target_schema: str


ConnectionInfoUnion = discriminated_union(
    ConsoleConnectionInfo,
    RedshiftS3ConnectionInfo,
    SnowflakeSMSConnectionInfo,
    BigQueryGCSServiceAccountConnectionInfo,
    PostgresConnectionInfo
)


class ScheduleModel(BaseModel):
    frequency: Literal["WEEKLY", "DAILY", "SIXHOURLY", "HOURLY"]
    type: Literal["INCREMENTAL", "FULL_REFRESH"]
    day: Optional[int] = None  # Required only if frequency == "WEEKLY"
    hour: Optional[int] = None
    minute: Optional[int] = None

    @field_validator("day")
    @classmethod
    def validate_day(cls, value, info):
        if info.data["frequency"] == "WEEKLY" and value is None:
            raise ValueError("The 'day' field is required when frequency is 'WEEKLY'.")
        return value


    def to_cron(self) -> str:
        minute = self.minute if self.minute is not None else 0
        hour = self.hour if self.hour is not None else 0

        if self.frequency == "WEEKLY":
            if self.day is None:
                raise ValueError("Day is required for weekly schedules")
            # self.day should be numeric (0-6 for Sunday-Saturday) or string (SUN, MON, ...)
            return f"{minute} {hour} * * {self.day}"

        elif self.frequency == "DAILY":
            return f"{minute} {hour} * * *"

        elif self.frequency == "SIXHOURLY":
            # run every 6 hours starting at hour 0 (i.e., 0,6,12,18)
            return f"{minute} */6 * * *"

        elif self.frequency == "HOURLY":
            return f"{minute} * * * *"

        raise ValueError("Invalid frequency")



class Destination:

    class Exception(Exception):
        pass

    class State:
        DRAFT = 'DRAFT'
        CREATED = 'CREATED'


    class Base(SQLModel):
        destination_name: str
        recipient_id: uuid.UUID
        vendor_type: str
        schedule: ScheduleModel
        models: List[uuid.UUID]
        is_enabled: bool = False


    class Public(Base):
        destination_id: uuid.UUID
        state: str
        created_at: datetime
        modified_at: datetime


    class Detail(Base):
        destination_id: uuid.UUID
        primary_transfer_id: Optional[uuid.UUID]
        state: str
        created_at: datetime
        modified_at: datetime
        connection_info: ConnectionInfoUnion
    

    class Create(Base):
        connection_info: ConnectionInfoUnion
    

    class Update(Base):
        destination_name: str | None = None
        recipient_id: uuid.UUID | None = None
        primary_transfer_id: uuid.UUID | None = None
        vendor_type: str | None = None
        schedule: ScheduleModel | None = None
        models: List[uuid.UUID] | None = None
        state: Literal["DRAFT", "CREATED"] | None = None
        is_enabled: bool | None = None
        connection_info: ConnectionInfoUnion | None = None


    class Model(Base, table=True):
        __tablename__ = "destination"
    
        destination_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
        recipient_id: uuid.UUID = Field(foreign_key="recipient.recipient_id")
        primary_transfer_id: Optional[uuid.UUID] = Field(default=None, foreign_key="transfer.transfer_id")
        schedule: dict = Field(sa_column=Column(JSON))
        models: List[str] = Field(sa_column=Column(JSON))
        connection_info: dict = Field(sa_column=Column(JSON))
        state: str
        created_by: uuid.UUID
        modified_by: uuid.UUID
        created_at: datetime = Field(default_factory=datetime.utcnow)
        modified_at: datetime = Field(default_factory=datetime.utcnow)



    @staticmethod
    def get(session, destination_id:uuid.UUID) -> Model:
        return session.get(Destination.Model, destination_id)
    
    
    @staticmethod
    def list(session, offset:int, limit:int, organization_id:uuid.UUID = None) -> List[Public]:
        stmt = (
            select(Destination.Model)
            .where(Destination.Model.state == Destination.State.CREATED)
            .offset(offset)
            .limit(limit)
        )
        if organization_id:
            stmt = (stmt
                    .join(Recipient.Model, Destination.Model.recipient_id == Recipient.Model.recipient_id)
                    .where(Recipient.Model.organization_id == organization_id)
            )
        return session.exec(stmt).all()


    @staticmethod
    def create(session, destination:Create, created_by:uuid.UUID) -> Model:
        
        new = Destination.Create.model_validate(destination)

        if new.vendor_type != new.connection_info.vendor_type:
            raise Destination.Exception('Invalid destination: vendor_type values do not match')

        if not session.get(Recipient.Model, new.recipient_id):
            raise Destination.Exception('Invalid destination: recipient does not exist')

        if len(new.models) <= 0:
            raise Destination.Exception('Invalid destination: no models specified')
        
        for model_id in new.models:
            if not session.get(Model.Model, model_id):
                raise Destination.Exception('Invalid destination: requested model does not exist')

        model = Destination.Model()  
        model.destination_name = new.destination_name
        model.recipient_id = new.recipient_id
        model.vendor_type = new.vendor_type
        model.schedule = json.loads(new.schedule.model_dump_json())
        model.models = [str(model_id) for model_id in new.models]
        model.state = Destination.State.DRAFT
        model.created_by = created_by
        model.modified_by = created_by
        model.connection_info = json.loads(
            new.connection_info.model_dump_json(context={'hide_sensitive': False})
        )
        
        session.add(model)
        session.commit()
        session.refresh(model)
        return model


    @staticmethod
    def update(session, destination_id:uuid.UUID, destination:Update, modified_by:uuid.UUID) -> Model:
        model = session.get(Destination.Model, destination_id)
        if not model:
            raise Destination.Exception('Destination does not exist')

        upd = Destination.Update.model_validate(destination)

        if upd.destination_name != None:
            model.destination_name = upd.destination_name
        
        if upd.schedule != None:
            model.schedule = json.loads(upd.schedule.model_dump_json())

        if upd.state != None:
            model.state = upd.state

        if upd.is_enabled != None:
            model.is_enabled = upd.is_enabled

        if upd.recipient_id != None:
            if not session.get(Recipient.Model, upd.recipient_id):
                raise Destination.Exception('Invalid update: recipient does not exist')
            else:
                model.recipient_id = upd.recipient_id

        if upd.primary_transfer_id != None:
            if not session.get(Transfer.Model, upd.primary_transfer_id):
                raise Destination.Exception('Invalid update: primary transfer does not exist')
            else:
                model.primary_transfer_id = upd.primary_transfer_id
        
        if upd.models != None:
            if len(upd.models) <= 0:
                raise Destination.Exception('Invalid update: empty model list')
        
            for model_id in upd.models:
                if not session.get(Model.Model, model_id):
                    raise Destination.Exception('Invalid update: requested model does not exist')
            
            model.models = [str(model_id) for model_id in upd.models]

        if upd.connection_info != None:
            if upd.connection_info.vendor_type != model.vendor_type:
                raise Destination.Exception('Invalid update: vendor_type must match existing')
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
    def clone(session, destination_id:uuid.UUID, created_by:uuid.UUID) -> Model:
        src = Destination.get(session, destination_id)
        if src == None:
            raise Destination.Exception('Destination to clone does not exist')
        if src.state == Destination.State.DRAFT:
            raise Destination.Exception('Attempting to clone a DRAFT state Destination')
        
        clone = Destination.Model()  
        clone.destination_name = src.destination_name
        clone.recipient_id = src.recipient_id
        clone.vendor_type = src.vendor_type
        clone.schedule = src.schedule
        clone.models = src.models
        clone.state = Destination.State.DRAFT
        clone.created_by = created_by
        clone.modified_by = created_by
        clone.connection_info = src.connection_info
        
        session.add(clone)
        session.commit()
        session.refresh(clone)
        return clone

    
    @staticmethod
    def delete(session, destination_id:uuid.UUID) -> bool:
        dest = session.get(Destination.Model, destination_id)
        if not dest:
            raise Destination.Exception('Destination does not exist')
        session.delete(dest)
        session.commit()
        return True
