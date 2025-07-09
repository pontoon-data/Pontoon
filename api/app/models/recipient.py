import uuid
from datetime import datetime, timezone
from typing import Optional, List
from sqlmodel import SQLModel, Field, select



class Recipient:


    class Exception(Exception): pass


    class Base(SQLModel):
        recipient_name: str
        tenant_id: str


    class Create(Base):
        pass


    class Update(Base):
        recipient_name: str | None = None
        tenant_id: str | None = None


    class Public(Base):
        recipient_id: uuid.UUID
        recipient_name: str
        tenant_id: str
        created_at: datetime
        modified_at: datetime


    class Model(Base, table=True):
        __tablename__ = "recipient"
        
        recipient_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
        organization_id: uuid.UUID = Field(foreign_key="organization.organization_id")
        created_by: uuid.UUID
        modified_by: uuid.UUID
        created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        modified_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


    @staticmethod
    def get(session, recipient_id:uuid.UUID) -> Public:
        return session.get(Recipient.Model, recipient_id)
    
    
    @staticmethod
    def list(session, offset:int, limit:int, organization_id:uuid.UUID = None) -> List[Public]:
        
        stmt = select(Recipient.Model).offset(offset).limit(limit)
        
        if organization_id:
            stmt = stmt.where(Recipient.Model.organization_id == organization_id)

        return session.exec(stmt).all()


    @staticmethod
    def create(session, recipient:Create, created_by:uuid.UUID, organization_id:uuid.UUID) -> Public:
        new = Recipient.Create.model_validate(recipient)
    
        model = Recipient.Model()
        model.recipient_name = new.recipient_name
        model.tenant_id = new.tenant_id
        model.created_by = created_by
        model.modified_by = created_by
        model.organization_id = organization_id
        
        session.add(model)
        session.commit()
        session.refresh(model)
        return model


    @staticmethod
    def update(session, recipient_id:uuid.UUID, recipient:Update, modified_by:uuid.UUID) -> Public:
        model = session.get(Recipient.Model, recipient_id)
        if not model:
            raise Recipient.Exception('Recipient does not exist')

        upd = Recipient.Update.model_validate(recipient)

        if upd.recipient_name != None:
            model.recipient_name = upd.recipient_name
        
        if upd.tenant_id != None:
            model.tenant_id = upd.tenant_id

        model.modified_by = modified_by
        model.modified_at = datetime.now(timezone.utc)

        session.add(model)
        session.commit()
        session.refresh(model)
        return model


    @staticmethod
    def delete(session, recipient_id:uuid.UUID) -> bool:
        recipient = session.get(Recipient.Model, recipient_id)
        if not recipient:
            raise Recipient.Exception('Recipient does not exist')
        session.delete(recipient)
        session.commit()
        return True