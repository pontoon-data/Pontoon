from typing import Annotated
from fastapi import HTTPException, Depends, Query, APIRouter, Security, status
from sqlmodel import Field, SQLModel, select, func
import uuid
import time

from app.dependencies import get_session, get_auth
from app.models import Auth, Recipient

router = APIRouter(
    prefix="/recipients",
    dependencies=[Depends(get_session), Security(get_auth)]
)


def get_recipient_by_id(session, recipient_id:uuid.UUID):
    recipient = Recipient.get(session, recipient_id)
    if not recipient:
        raise HTTPException(status_code=404, detail="Recipient not found")
    return recipient


@router.post("", response_model=Recipient.Public, status_code=status.HTTP_201_CREATED)
def create_recipient(recipient: Recipient.Create, session=Depends(get_session), auth:Auth = Security(get_auth)):
    return Recipient.create(
        session,
        recipient,
        created_by=auth.sub_uuid(),
        organization_id=auth.org_uuid()
    )


@router.get("", response_model=list[Recipient.Public])
def read_recipients(
        session = Depends(get_session),
        auth:Auth = Security(get_auth),
        offset: int = 0,
        limit: Annotated[int, Query(le=100)] = 100,
):
    return Recipient.list(session, offset, limit, auth.org_uuid())


@router.get("/{recipient_id}", response_model=Recipient.Public)
def read_recipient(recipient_id: uuid.UUID, session=Depends(get_session)):
    return get_recipient_by_id(session, recipient_id)


@router.put("/{recipient_id}", response_model=Recipient.Public)
def update_recipient(recipient_id:uuid.UUID, recipient:Recipient.Update, session=Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        return Recipient.update(
            session,
            recipient_id,
            recipient,
            modified_by=auth.sub_uuid()
        )
    except Recipient.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{recipient_id}")
def delete_recipients(
    recipient_id: uuid.UUID, 
    session = Depends(get_session)
):
    try:
        Recipient.delete(session, recipient_id)
        return {"ok": True}
    except Recipient.Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
