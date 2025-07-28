import uuid
from datetime import datetime, timezone
from typing import Optional, List, Literal
from pydantic import BaseModel
from sqlmodel import SQLModel, Field, Column, JSON, select



class Transfer:

    class Exception(Exception): pass


    class Base(SQLModel):
        destination_id: uuid.UUID
    
    
    class Create(Base):
        pass

    
    class Update(Base):
        destination_id: uuid.UUID | None = None


    class Model(Base, table=True):
        __tablename__ = "transfer"

        transfer_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
        destination_id: uuid.UUID = Field(foreign_key="destination.destination_id")
        created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        modified_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        

    @staticmethod
    def get(session, transfer_id:uuid.UUID) -> Model:
        return session.get(Transfer.Model, transfer_id)
    

    @staticmethod
    def get_by_destination_id(session, destination_id:uuid.UUID) -> List[Model]:
        stmt = (
            select(Transfer.Model)
            .where(Transfer.Model.destination_id == destination_id)
        )
        return session.exec(stmt).all()

    
    @staticmethod
    def list(session, offset:int, limit:int) -> List[Model]:
        return session.exec(select(Transfer.Model).offset(offset).limit(limit)).all()


    @staticmethod
    def create(session, transfer:Create) -> Model:
        new = Transfer.Create.model_validate(transfer)
    
        model = Transfer.Model()
        model.destination_id = new.destination_id
        
        session.add(model)
        session.commit()
        session.refresh(model)
        return model


    @staticmethod
    def update(session, transfer_id:uuid.UUID, transfer:Update) -> Model:
        raise Transfer.Exception('Transfer.update() is not implemented')


    @staticmethod
    def delete(session, transfer_id:uuid.UUID) -> bool:
        transfer = session.get(Transfer.Model, transfer_id)
        if not transfer:
            raise Transfer.Exception('Transfer does not exist')
        session.delete(transfer)
        session.commit()
        return True

