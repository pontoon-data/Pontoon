import uuid
from fastapi import HTTPException, Depends, Query, APIRouter

from app.dependencies import get_session
from app.models import Destination, Model, Recipient, Source, Transfer, TransferRun
from app.routers.recipients import get_recipient_by_id
from app.routers.sources import get_source_by_id
from app.routers.models import get_model_by_id
from app.routers.destinations import get_destination_by_id
from app.dependencies import send_telemetry_event


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
    return TransferRun.get_latest_transfer_run(session, transfer_id)


@router.post("/runs", response_model=TransferRun.Model)
def create_transfer_run(transfer_run:TransferRun.Create, session=Depends(get_session)):
    return TransferRun.create(session, transfer_run)


@router.put("/runs/{transfer_run_id}", response_model=TransferRun.Model)
def update_transfer_run(transfer_run_id:uuid.UUID, transfer_run:TransferRun.Update, session=Depends(get_session)):
    transfer_run = TransferRun.update(session, transfer_run_id, transfer_run)
    
    if transfer_run.meta is None:
        return transfer_run

    transfer_run_type = transfer_run.meta.get('arguments', {}).get('type')
    if transfer_run_type != "transfer" or transfer_run.status == 'RUNNING':
        return transfer_run

    # Get the number of rows transferred
    row_count = TransferRun.get_transfer_row_count(session, transfer_run_id)

    # Get the destination vendor type
    transfer_id = transfer_run.transfer_id
    destination_id = Transfer.get(session, transfer_id).destination_id
    destination_vendor_type = Destination.get(session, destination_id).connection_info['vendor_type']

    # Get the source vendor types from the models transferred
    models = Destination.get(session, destination_id).models
    source_ids = { Model.get(session, model_id).source_id for model_id in models }
    source_vendor_types = { Source.get(session, source_id).connection_info['vendor_type'] for source_id in source_ids }

    if transfer_run_type == "transfer" and transfer_run.status == 'SUCCESS':
        send_telemetry_event(
            "transfer_run_success",
            properties={
                "rows_transferred": row_count,
                "destination_vendor_type": destination_vendor_type,
                "source_vendor_types": source_vendor_types
            }
        )
    elif transfer_run_type == "transfer" and transfer_run.status == 'FAILURE':
        send_telemetry_event(
            "transfer_run_failure",
            properties={
                "rows_transferred": row_count,
                "destination_vendor_type": destination_vendor_type,
                "source_vendor_types": source_vendor_types
            }
        )
    return transfer_run


