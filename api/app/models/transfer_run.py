import uuid
from datetime import datetime, timezone
from typing import Optional, List, Literal
from pydantic import BaseModel
from sqlmodel import SQLModel, Field, Column, JSON, select

from app.models.transfer import Transfer


class TransferRun:

    class Exception(Exception): pass


    class Base(SQLModel):
        transfer_id: uuid.UUID
        meta: dict
        status: Literal["RUNNING", "SUCCESS", "FAILURE"]


    class Create(Base):
        pass
    

    class Update(Base):
        transfer_id: uuid.UUID | None = None
        meta: dict | None = None
        output: dict | None = None


    class Model(Base, table=True):
        __tablename__ = "transfer_run"

        transfer_run_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
        transfer_id: uuid.UUID = Field(foreign_key="transfer.transfer_id")
        status: str
        meta: dict = Field(sa_column=Column(JSON))
        output: dict = Field(sa_column=Column(JSON))
        created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
        modified_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


    @staticmethod
    def get(session, transfer_run_id:uuid.UUID) -> Model:
        stmt = (
            select(TransferRun.Model)
            .where(TransferRun.Model.transfer_run_id == transfer_run_id)
        )
        results = session.exec(stmt).all()
        if len(results) == 1:
            return results[0]
        elif len(results) == 0:
            raise TransferRun.Exception("Transfer Run not found")
        else:
            raise TransferRun.Exception(f"Multiple transfer runs found for id {transfer_run_id}")
    
    @staticmethod
    def get_latest_transfer_run(session, transfer_id:uuid.UUID, status:str = None) -> Model:
        stmt = (
            select(TransferRun.Model)
            .where(TransferRun.Model.transfer_id == transfer_id)
            .order_by(TransferRun.Model.created_at.desc())
            .limit(1)
        )
        
        if status != None:
            stmt = stmt.where(TransferRun.Model.status == status)

        return session.exec(stmt).first()


    @staticmethod
    def list(session, destination_id:uuid.UUID, offset:int, limit:int) -> List[Model]: 
        stmt = (
            select(TransferRun.Model)
            .join(Transfer.Model, TransferRun.Model.transfer_id == Transfer.Model.transfer_id)
            .where(Transfer.Model.destination_id == destination_id)
            .order_by(TransferRun.Model.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        return session.exec(stmt).all()


    @staticmethod
    def create(session, transfer_run:Create) -> Model:
        new = TransferRun.Create.model_validate(transfer_run)
    
        model = TransferRun.Model()
        model.transfer_id = new.transfer_id
        model.status = new.status
        model.meta = new.meta

        session.add(model)
        session.commit()
        session.refresh(model)
        return model


    @staticmethod
    def update(session, transfer_run_id:uuid.UUID, transfer_run:Update) -> Model:
        model = session.get(TransferRun.Model, transfer_run_id)
        if not model:
            raise TransferRun.Exception('Transfer run does not exist')

        upd = TransferRun.Update.model_validate(transfer_run)

        if upd.status != None:
            model.status = upd.status

        if upd.output != None:
            model.output = upd.output

        if upd.meta != None:
            model.meta = upd.meta

        model.modified_at = datetime.now(timezone.utc)

        session.add(model)
        session.commit()
        session.refresh(model)
        return model

    @staticmethod
    def get_transfer_row_count(session, transfer_run_id:uuid.UUID) -> int | None:
        """
        Get the total number of rows transferred for a specific transfer run. This only counts rows written to a destination.
        
        Args:
            session: Database session
            transfer_run_id: UUID of the transfer run
            
        Returns:
            int | None: Total number of rows transferred, or None if no progress data available
            
        Raises:
            TransferRun.Exception: If transfer run is not found
        """
        transfer_run = TransferRun.get(session, transfer_run_id)
        
        if transfer_run.output is None:
            return None
            
        progress_data = transfer_run.output.get('progress', None)
        if not progress_data:
            return None
            
        total_rows = 0
        
        # Sum up the processed rows from all progress entities
        for entity_progress in progress_data.values():
            if isinstance(entity_progress, dict) and 'processed' in entity_progress:
                entity = entity_progress.get('entity')
                # Only count rows written to a destination
                if isinstance(entity, str) and entity.startswith('destination'):
                    processed = entity_progress.get('processed', 0)
                    if isinstance(processed, (int, float)):
                        total_rows += int(processed)
    
        return total_rows 
