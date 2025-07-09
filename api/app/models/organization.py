import uuid
from datetime import datetime, timezone
from typing import Optional, List
from sqlmodel import SQLModel, Field, select



class Organization:

    class Base(SQLModel):
        organization_name: str


    class Create(Base):
        pass


    class Update(Base):
        organization_name: str | None = None
        

    class Public(Base):
        organization_id: uuid.UUID
        organization_name: str
        created_at: datetime
        modified_at: datetime


    class Model(Base, table=True):
        __tablename__ = "organization"
        
        organization_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
        created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        modified_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))



    @staticmethod
    def get(session, organization_id:uuid.UUID) -> Public:
        return session.get(Organization.Model, recipient_id)
    
    
    @staticmethod
    def list(session, offset:int, limit:int) -> List[Public]:
        return session.exec(select(Organization.Model).offset(offset).limit(limit)).all()


    @staticmethod
    def create(session, org:Create) -> Public:
        model = Organization.Model(**org.dict())
    
        # TODO

        session.add(model)
        session.commit()
        session.refresh(model)
        return model


    @staticmethod
    def update(session, org:Update) -> Public:
        pass


    @staticmethod
    def delete(session, organization_id:uuid.UUID) -> bool:
        org = session.get(Organization.Model, organization_id)
        if not org:
            return False
        session.delete(org)
        session.commit()
        return True