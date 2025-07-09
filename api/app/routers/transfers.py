import uuid
from typing import Annotated
from fastapi import HTTPException, Depends, Query, APIRouter, Security, status
from datetime import datetime

from app.dependencies import get_session, get_settings, get_auth
from app.models import Auth, TransferRun, Destination, Recipient, Transfer as TransferModel

from pontoon import Mode
from pontoon.orchestration.client import Transfer, TransferException


settings = get_settings()


router = APIRouter(
    prefix="/transfers",
    dependencies=[Depends(get_session), Security(get_auth)]
)


Transfer.configure(
    schedule_name_prefix = f"pontoon-{settings.env}-"
)


@router.get("", response_model=list[TransferRun.Model])
def search_transfers(
        destination_id: uuid.UUID,
        session = Depends(get_session),
        offset: int = 0,
        limit: Annotated[int, Query(le=100)] = 100,
):
    return TransferRun.list(session, destination_id, offset, limit)


@router.post("/{transfer_run_id}/rerun")
def rerun_transfer(
        transfer_run_id:uuid.UUID,
        session = Depends(get_session),
        auth:Auth = Security(get_auth)
):
    try:
        
        # run some checks + balances
        transfer_run = session.get(TransferRun.Model, transfer_run_id)
        if transfer_run == None:
            raise TransferRun.Exception("Transfer run not found")
        
        transfer = TransferModel.get(session, transfer_run.transfer_id)
        if transfer == None:
            raise TransferRun.Exception("Transfer run parent Transfer not found")

        destination = Destination.get(session, transfer.destination_id)
        if destination == None:
            raise TransferRun.Exception("Transfer Destination not found")
            
        recipient = Recipient.get(session, destination.recipient_id)
        if recipient == None:
            raise TransferRun.Exception("Destination recipient not found")

        if recipient.organization_id != auth.org_uuid():
            raise TransferRun.Exception("Recipient org ID does not match session.")
    
        # get the inputs from the last run 
        meta = transfer_run.meta
        if not meta.get('arguments', {}):
            raise TransferRun.Exception("Transfer run does not have arguments; can't re-run")

        args = meta.get('arguments', {})

        # kick off a new transfer
        if settings.skip_transfers != True:
            new_run = Transfer.create(str(transfer_run.transfer_id))
            new_run.set_schedule(Transfer.NOW)
            new_run.set_organization(auth.org_uuid())
            new_run.set_destination(str(destination.destination_id))
            new_run.set_mode(Mode(args.get('mode', {})))
            new_run.set_models(args.get('models', []))
            new_run.run(expedited=False)

        return {"ok": True}


    except (TransferException, TransferRun.Exception):
        raise HTTPException(status_code=400, detail=str(e))

