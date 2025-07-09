import uuid
from datetime import datetime, timezone
from typing import Optional, List, Literal
from sqlmodel import SQLModel, Field, Column, JSON, select



class Task:


    class Exception(Exception): pass

    class Base(SQLModel):
        output: dict
        status: Literal["RUNNING", "COMPLETE"]


    class Create(Base):
        meta: dict
    

    class Public(Base):
        task_id: uuid.UUID
        created_at: datetime
        updated_at: datetime


    class Update(Base):
        meta: dict | None = None
        output: dict | None = None


    class Model(Base, table=True):
        __tablename__ = "task"
        
        task_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
        meta: dict = Field(sa_column=Column(JSON))
        output: dict = Field(sa_column=Column(JSON))
        status: str
        organization_id: uuid.UUID
        created_by: uuid.UUID
        created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


    @staticmethod
    def get(session, task_id:uuid.UUID) -> Model:
        return session.get(Task.Model, task_id)
    

    @staticmethod
    def create(session, task:Create, created_by:uuid.UUID, organization_id:uuid.UUID) -> Model:
        new = Task.Create.model_validate(task)
    
        model = Task.Model()
        model.meta = new.meta
        model.status = new.status
        model.output = new.output
        model.created_by = created_by
        model.organization_id = organization_id
        
        session.add(model)
        session.commit()
        session.refresh(model)
        return model


    @staticmethod
    def update(session, task_id:uuid.UUID, task:Update) -> Model:
        model = session.get(Task.Model, task_id)
        if not model:
            raise Task.Exception('Task does not exist')

        upd = Task.Update.model_validate(task)

        if upd.status != None:
            model.status = upd.status

        if upd.meta != None:
            model.meta = upd.meta
        
        if upd.output != None:
            model.output = upd.output

        model.updated_at = datetime.now(timezone.utc)

        session.add(model)
        session.commit()
        session.refresh(model)
        return model