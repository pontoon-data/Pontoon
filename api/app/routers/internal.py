import uuid
from fastapi import HTTPException, Depends, Query, APIRouter

from app.dependencies import get_session
from app.models import Destination, Model, Recipient, Source, Transfer, TransferRun
from app.routers.recipients import get_recipient_by_id
from app.routers.sources import get_source_by_id
from app.routers.models import get_model_by_id
from app.routers.destinations import get_destination_by_id


router = APIRouter(
    prefix="/internal",
    dependencies=[Depends(get_session)]
)


@router.get("/recipients/{recipient_id}", response_model=Recipient.Public)
def read_recipient(recipient_id: uuid.UUID, session=Depends(get_session)):
    return get_recipient_by_id(session, recipient_id)


@router.get("/sources/{source_id}", response_model=Source.Model)
def read_source(source_id: uuid.UUID, session=Depends(get_session)):
    return get_source_by_id(session, source_id)


@router.get("/models/{model_id}", response_model=Model.Public)
def read_model(model_id: uuid.UUID, session=Depends(get_session)):
    return get_model_by_id(session, model_id)


@router.get("/destinations/{destination_id}", response_model=Destination.Model)
def read_destination(destination_id: uuid.UUID, session=Depends(get_session)):
    return get_destination_by_id(session, destination_id)


@router.get("/runs/{transfer_id}", response_model=TransferRun.Model)
def get_transfer_run(transfer_id: uuid.UUID, session=Depends(get_session)):
    return Transfer.get_latest_transfer_run(session, transfer_id)


@router.post("/runs", response_model=TransferRun.Model)
def create_transfer_run(transfer_run:TransferRun.Create, session=Depends(get_session)):
    return TransferRun.create(session, transfer_run)


@router.put("/runs/{transfer_run_id}", response_model=TransferRun.Model)
def update_transfer_run(transfer_run_id:uuid.UUID, transfer_run:TransferRun.Update, session=Depends(get_session)):
    return TransferRun.update(session, transfer_run_id, transfer_run)


