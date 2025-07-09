import uuid
from datetime import datetime, timezone
from typing import Optional, List
from sqlmodel import SQLModel, Field, Column, JSON, select, func

from app.models.source import Source


class Model:


    class Exception(Exception): pass


    class Base(SQLModel):
        source_id: uuid.UUID
        model_name: str
        model_description: str
        schema_name: str
        table_name: str
        include_columns: List[dict]
        primary_key_column: str
        tenant_id_column: str
        last_modified_at_column: Optional[str]


    class Create(Base):
        pass
        

    class Update(Base):
        source_id: uuid.UUID | None = None
        model_name: str | None = None
        model_description: str | None = None
        schema_name: str | None = None
        table_name: str | None = None
        include_columns: List[dict] | None = None
        primary_key_column: str | None = None
        tenant_id_column: str | None = None
        last_modified_at_column: str | None = None


    class Public(Base):
        model_id: uuid.UUID
        created_at: datetime
        modified_at: datetime
        

    class Model(Base, table=True):
        __tablename__ = "model"
    
        model_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
        source_id: uuid.UUID = Field(foreign_key="source.source_id")
        include_columns: List[dict] = Field(sa_column=Column(JSON))
        created_by: uuid.UUID
        modified_by: uuid.UUID
        created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        modified_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


    @staticmethod
    def get(session, model_id:uuid.UUID) -> Public:
        return session.get(Model.Model, model_id)
    
    
    @staticmethod
    def list(session, offset:int, limit:int, organization_id:uuid.UUID) -> List[Public]:
        stmt = select(Model.Model).offset(offset).limit(limit)
        if organization_id:
            stmt = (stmt
                    .join(Source.Model, Model.Model.source_id == Source.Model.source_id)
                    .where(Source.Model.organization_id == organization_id)
            )
        return session.exec(stmt).all()


    @staticmethod
    def create(session, model:Create, created_by:uuid.UUID) -> Public:
        create = Model.Create.model_validate(model)

        # check that source exists
        statement = select(func.count(Source.Model.source_id)).where(
            Source.Model.source_id == create.source_id
        )
        results = session.exec(statement).one()
        if results == 0:
            raise Model.Exception("Parent source does not exist")
    
        new = Model.Model()
        new.source_id = create.source_id
        new.model_name = create.model_name
        new.model_description = create.model_description 
        new.schema_name = create.schema_name
        new.table_name = create.table_name
        new.include_columns = create.include_columns
        new.primary_key_column = create.primary_key_column
        new.tenant_id_column = create.tenant_id_column
        new.last_modified_at_column = create.last_modified_at_column
        new.created_by = created_by
        new.modified_by = created_by
        
        session.add(new)
        session.commit()
        session.refresh(new)
        return new 


    @staticmethod
    def update(session, model_id:uuid.UUID, model:Update, modified_by:uuid.UUID) -> Public:
        existing = session.get(Model.Model, model_id)
        if not existing:
            raise Model.Exception('Model does not exist')

        upd = Model.Update.model_validate(model)

        if upd.model_name != None:
            existing.model_name = upd.model_name
        
        if upd.model_description != None:
            existing.model_description = upd.model_description

        if upd.schema_name != None:
            existing.schema_name = upd.schema_name
        
        if upd.table_name != None:
            existing.table_name = upd.table_name

        if upd.include_columns != None:
            existing.include_columns = upd.include_columns
        
        if upd.primary_key_column != None:
            existing.primary_key_column = upd.primary_key_column

        if upd.tenant_id_column != None:
            existing.tenant_id_column = upd.tenant_id_column
        
        if upd.last_modified_at_column != None:
            existing.last_modified_at_column = upd.last_modified_at_column

        existing.modified_by = modified_by
        existing.modified_at = datetime.now(timezone.utc)

        session.add(existing)
        session.commit()
        session.refresh(existing)
        return existing


    @staticmethod
    def delete(session, model_id:uuid.UUID) -> bool:
        model = session.get(Model.Model, model_id)
        if not model:
            raise Model.Exception('Model does not exist')
        session.delete(model)
        session.commit()
        return True