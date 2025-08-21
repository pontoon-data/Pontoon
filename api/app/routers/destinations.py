import uuid
import time
from typing import Annotated, List, Optional, Literal
from datetime import datetime, timezone
from fastapi import HTTPException, Depends, Query, APIRouter, Security, status
from pydantic import BaseModel

from app.models import Auth, Task, Destination, ScheduleModel, TransferRun, Transfer as TransferModel
from app.routers.common import create_transfer_task, transfer_task_status
from app.dependencies import get_session, get_settings, get_auth, send_telemetry_event
from app.config import Settings

from pontoon import Mode
from pontoon.orchestration.client import Transfer, TransferException


settings = get_settings()

router = APIRouter(
    prefix="/destinations",
    dependencies=[Depends(get_session), Security(get_auth)]
)

Transfer.configure(
    schedule_name_prefix = f"pontoon-{settings.env}-"
)


def get_destination_by_id(session, destination_id:uuid.UUID):
    return Destination.get(session, destination_id)


class TransferScheduleOverride(BaseModel):
    type: Literal["FULL_REFRESH", "INCREMENTAL"]
    start: Optional[datetime] = None
    end: Optional[datetime] = None

#
# Transfer() management helpers
#

def create_or_update_transfer(session, destination:Destination.Model, destination_update:Destination.Update, auth:Auth):

    if destination.primary_transfer_id:
        # we already have a scheduled transfer, update it
        print(f"Updating trasfer for destination_id {destination.destination_id}")
        update_transfer(session, destination, destination_update, auth)
    else:
        # we don't have a scheduled transfer yet
        print(f"Creating trasfer for destination_id {destination.destination_id}")
        if destination.state == Destination.State.CREATED and destination.is_enabled == True:
            # if the destination is enabled, create one
            create_transfer(session, destination, auth) 



def create_transfer(session, destination:Destination.Model, auth:Auth):
    
    # create a transfer entry
    transfer_model = TransferModel.create(
        session, 
        TransferModel.Create(destination_id=destination.destination_id)
    )

    # create our scheduled transfer
    if settings.skip_transfers == False:
        transfer = Transfer.create(str(transfer_model.transfer_id))
        transfer.set_organization(auth.org_uuid())
        transfer.set_destination(str(transfer_model.destination_id))
        schedule_model = ScheduleModel.model_validate(destination.schedule)
        transfer.set_schedule(schedule_model.to_cron())
        transfer.apply()

    # make transfer the destination primary
    Destination.update(
        session, 
        destination.destination_id, 
        Destination.Update(primary_transfer_id=transfer_model.transfer_id),
        auth.sub_uuid()
    )

    # kick off an initial run
    if settings.skip_transfers == False:
        backfill = Transfer.create(str(transfer_model.transfer_id))
        backfill.set_organization(auth.org_uuid())
        backfill.set_destination(str(transfer_model.destination_id)) 
        backfill.set_mode(Mode({'type': 'FULL_REFRESH'}))
        backfill.run(expedited=False)


def update_transfer(session, destination:Destination.Model, destination_update:Destination.Update, auth:Auth):
    
    # not strictly necessary to fetch this, but use as integrity check 
    transfer_model = TransferModel.get(session, destination.primary_transfer_id)

    if settings.skip_transfers == True:
        return

    transfer = Transfer(str(transfer_model.transfer_id))

    if not transfer.exists():
        raise Destination.Exception("Destination primary transfer does not exist")

    # change schedule    
    if destination_update.schedule != None:
        transfer.set_schedule(destination_update.schedule.to_cron())
        transfer.apply()

    # enable/disable schedule
    if destination_update.is_enabled != None:
        transfer_is_enabled = transfer.is_enabled()
        if destination_update.is_enabled == True and transfer_is_enabled == False:
            transfer.enable()
        elif destination_update.is_enabled == False and transfer_is_enabled == True:
            transfer.disable()


def delete_transfer(session, destination_id:uuid.UUID):

    destination = get_destination_by_id(session, destination_id)

    if destination.primary_transfer_id == None:
        return

    transfer_model = TransferModel.get(session, destination.primary_transfer_id)

    # delete the scheduled transfer
    if settings.skip_transfers == False:
        transfer = Transfer(str(transfer_model.transfer_id))
        if transfer.exists():
            transfer.delete()

    # delete the transfer <> destination link
    TransferModel.delete(session, transfer_model.transfer_id)


#
# Router endpoints
#

@router.get("", response_model=List[Destination.Public])
def list_destinations(session = Depends(get_session), auth:Auth = Security(get_auth), offset:int = 0, limit:Annotated[int, Query(le=100)] = 100):
    return Destination.list(session, offset, limit, auth.org_uuid())


@router.post("", response_model=Destination.Public, status_code=status.HTTP_201_CREATED)
def create_destination(destination:Destination.Create, session=Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        created_destination = Destination.create(
            session,
            destination,
            created_by=auth.sub_uuid()
        )
        
        # Send telemetry event
        send_telemetry_event(
            "destination_created",
            properties={
                "vendor_type": created_destination.vendor_type
            }
        )
        
        return created_destination

    except Destination.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{destination_id}/clone", response_model=Destination.Public, status_code=status.HTTP_201_CREATED)
def clone_destination(destination_id:uuid.UUID, session=Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        model = Destination.clone(
            session,
            destination_id,
            created_by=auth.sub_uuid()
        )
        
        return model

    except Destination.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{destination_id}", response_model=Destination.Detail)
def update_destination(destination_id:uuid.UUID, destination:Destination.Update, session=Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        model = Destination.update(
            session,
            destination_id,
            destination,
            modified_by=auth.sub_uuid()
        )
        
        create_or_update_transfer(session, model, destination, auth)

        return model
    except (Destination.Exception, TransferModel.Exception, TransferException) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{destination_id}", response_model=Destination.Detail)
def get_destination(destination_id:uuid.UUID, session=Depends(get_session)):
    return get_destination_by_id(session, destination_id)


@router.post("/{destination_id}/check", response_model=Task.Public)
def create_destination_check(destination_id:uuid.UUID, session=Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        if settings.skip_transfers:
            return create_transfer_task(
                session,
                'destination-check',
                str(uuid.uuid4()),
                auth,
                meta={'destination_id': str(destination_id)},
                status='COMPLETE',
                output={"success": True}
            )

        transfer_id = Transfer.test_destination(
            organization_id=auth.org_uuid(),
            destination_id=destination_id,
            run_async=True
        )

        return create_transfer_task(
            session,
            'destination-check',
            str(transfer_id),
            auth,
            meta={'destination_id': str(destination_id)}
        )
    except (TransferException, Task.Exception):
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{destination_id}/run")
def run_destination_transfer(
    destination_id: uuid.UUID, 
    schedule_override: Optional[TransferScheduleOverride] = None,
    session=Depends(get_session), 
    auth:Auth = Security(get_auth)
):
    try:
        if settings.skip_transfers:
            return {"ok": True}
        
        destination = get_destination_by_id(session, destination_id)
        if destination is None:
            raise HTTPException(status_code=400, detail="Destination does not exist")
        
        if destination.primary_transfer_id == None:
            raise HTTPException(status_code=400, detail="Destination does not have a primary transfer ID set")

        now = datetime.now(timezone.utc)
        schedule_model = ScheduleModel.model_validate(destination.schedule)

        run = Transfer.create(str(destination.primary_transfer_id))
        run.set_organization(auth.org_uuid())
        run.set_destination(str(destination_id)) 
        
        # Determine the mode based on request body or fall back to schedule defaults
        if schedule_override:
            if schedule_override.type == 'FULL_REFRESH':
                run.set_mode(Mode({'type': 'FULL_REFRESH'}))
            else:
                end_time = schedule_override.end if schedule_override.end else now
                start_time = schedule_override.start if schedule_override.start else (end_time - Mode.delta(schedule_model.frequency))
                run.set_mode(Mode({
                    'type': 'INCREMENTAL',
                    'period': schedule_model.frequency,
                    'start': start_time,
                    'end': end_time
                }))
        else:
            # Use schedule defaults
            if schedule_model.type != 'FULL_REFRESH':
                run.set_mode(Mode({
                    'type': schedule_model.type, 
                    'period': schedule_model.frequency, 
                    'start': now - Mode.delta(schedule_model.frequency), 
                    'end': now
                }))
        
        run.run(expedited=False)
    
    except TransferException as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{destination_id}/check/{task_id}", response_model=Task.Public)
def destination_check_status(destination_id:uuid.UUID, task_id:uuid.UUID, session=Depends(get_session)):
    task = transfer_task_status(session, task_id)

    if task.meta.get('type') != 'destination-check':
        raise HTTPException(status_code=400, detail="Task is not a destination check")
    
    if task.meta.get('destination_id') != str(destination_id):
        raise HTTPException(status_code=400, detail="Task does not belong to this destination")
    
    destination = Destination.get(session, destination_id)
    if task.status == 'COMPLETE' and task.output.get('success') == True:
        try:
            send_telemetry_event(
                "destination_test_connection_success",
                properties={
                    "vendor_type": destination.vendor_type
                }
            )
        except Exception as e:
            print(f"Telemetry error in destination check status: {e}")
    
    if task.status == 'COMPLETE' and task.output.get('success') == False:
        send_telemetry_event(
            "destination_test_connection_failure",
            properties={
                "vendor_type": destination.vendor_type
            }
        )

    return task


@router.delete("/{destination_id}")
def delete_destination(destination_id:uuid.UUID, session=Depends(get_session)):
    try:
        delete_transfer(session, destination_id)
        Destination.delete(session, destination_id)
        return {"ok": True}
    except (Destination.Exception, TransferModel.Exception, TransferException) as e:
        raise HTTPException(status_code=400, detail=str(e))

