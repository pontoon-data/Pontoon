import time
import uuid
import json
from typing import Annotated, List
from datetime import datetime, timezone
from fastapi import HTTPException, Depends, Query, status, APIRouter, Security
from sqlmodel import Session

from app.dependencies import get_session, get_settings, get_auth, send_telemetry_event
from app.models import Auth, Task, Source
from app.routers.common import create_transfer_task, transfer_task_status
from pontoon.orchestration.client import Transfer, TransferException

settings = get_settings()

router = APIRouter(
    prefix="/sources",
    dependencies=[Depends(get_session), Security(get_auth)]
)


Transfer.configure(
    schedule_name_prefix = f"pontoon-{settings.env}-"
)



def get_source_by_id(session, source_id:uuid.UUID):
    return Source.get(session, source_id)


@router.get("", response_model=List[Source.Public])
def list_sources(session:Session = Depends(get_session), auth:Auth = Security(get_auth), offset:int = 0, limit:Annotated[int, Query(le=100)] = 100):
    return Source.list(session, offset, limit, auth.org_uuid())


@router.post("", response_model=Source.Public, status_code=status.HTTP_201_CREATED)
def create_source(source:Source.Create, session:Session = Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        created_source = Source.create(
            session,
            source,
            created_by=auth.sub_uuid(),
            organization_id=auth.org_uuid()
        )
        
        # Send telemetry event
        send_telemetry_event(
            "source_created",
            properties={
                "vendor_type": created_source.vendor_type
            }
        )
        
        return created_source
    except Source.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{source_id}/clone", response_model=Source.Public, status_code=status.HTTP_201_CREATED)
def clone_source(source_id:uuid.UUID, session:Session = Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        return Source.clone(
            session,
            source_id,
            created_by=auth.sub_uuid()
        )
    except Source.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{source_id}", response_model=Source.Detail)
def update_source(source_id:uuid.UUID, source:Source.Update, session:Session = Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        return Source.update(
            session,
            source_id,
            source,
            modified_by=auth.sub_uuid()
        )
    except Source.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{source_id}", response_model=Source.Detail)
def get_source(source_id:uuid.UUID, session:Session = Depends(get_session)):
    source = get_source_by_id(session, source_id)
    if source == None:
        raise HTTPException(status_code=400, detail="Source does not exist")
    else:
        return source


@router.delete("/{source_id}")
def delete_source(source_id:uuid.UUID, session:Session = Depends(get_session)):
    try:
        Source.delete(session, source_id)
        return {"ok": True}
    except Source.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{source_id}/check", response_model=Task.Public)
def create_source_check(source_id:uuid.UUID, session=Depends(get_session), auth:Auth = Security(get_auth)):
    try:

        if settings.skip_transfers:
            return create_transfer_task(
                session,
                'source-check',
                str(uuid.uuid4()),
                auth,
                meta={'source_id': str(source_id)},
                status='COMPLETE',
                output={"success": True}
            )

        # transfer job to execute the check
        transfer_id = Transfer.test_source(
            organization_id=auth.org_uuid(),
            source_id=source_id,
            run_async=True
        )

        # API task that tracks a transfer
        return create_transfer_task(
            session,
            'source-check',
            str(transfer_id),
            auth,
            meta={'source_id': str(source_id)}
        )
    except (TransferException, Task.Exception):
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{source_id}/check/{task_id}", response_model=Task.Public)
def source_check_status(source_id:uuid.UUID, task_id:uuid.UUID, session=Depends(get_session)):
    task = transfer_task_status(session, task_id)
    
    if task.meta.get('type') != 'source-check':
        raise HTTPException(status_code=400, detail="Task is not a source check")
    
    if task.meta.get('source_id') != str(source_id):
        raise HTTPException(status_code=400, detail="Task does not belong to this source")
    
    source = Source.get(session, source_id)
    if task.status == 'COMPLETE' and task.output.get('success') == True:
        send_telemetry_event(
            "source_test_connection_success",
            properties={
                "vendor_type": source.vendor_type
            }
        )
    elif task.status == 'COMPLETE' and task.output.get('success') == False:
        send_telemetry_event(
            "source_test_connection_failure",
            properties={
                "vendor_type": source.vendor_type
            }
        ) 

    return task


@router.post("/{source_id}/metadata", response_model=Task.Public)
def get_source_metadata(source_id:uuid.UUID, session=Depends(get_session), auth:Auth = Security(get_auth)):
    try:

        if settings.skip_transfers:
            return create_transfer_task(
                session,
                'source-inspect',
                str(uuid.uuid4()),
                auth,
                meta={'source_id': str(source_id)},
                status='COMPLETE',
                output={
                    "success": True, 
                    "stream_info": json.loads(Source.MetaData.mock(source_id).model_dump_json())
                }
            )

        # transfer job to fetch metadata
        transfer_id = Transfer.inspect_source(
            organization_id=auth.org_uuid(),
            source_id=str(source_id),
            run_async=True
        )

        # API task to track the transfer job
        return create_transfer_task(
            session,
            'source-inspect',
            str(transfer_id),
            auth,
            meta={'source_id': str(source_id)}
        )
    except (TransferException, Task.Exception):
        raise HTTPException(status_code=400, detail=str(e))

    
@router.get("/{source_id}/metadata/{task_id}", response_model=Task.Public)
def source_metadata_status(source_id:uuid.UUID, task_id:uuid.UUID, session=Depends(get_session)):
    return transfer_task_status(session, task_id)
    