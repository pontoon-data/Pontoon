import time
import uuid
from typing import List, Annotated
from fastapi import HTTPException, Depends, Query, APIRouter, Security, status

from app.dependencies import get_session, get_auth, send_telemetry_event
from app.models import Auth, Model

router = APIRouter(
    prefix="/models",
    dependencies=[Depends(get_session), Security(get_auth)]
)


def get_model_by_id(session, model_id:uuid.UUID):
    try:
        return Model.get(session, model_id)
    except Model.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("", response_model=Model.Public, status_code=status.HTTP_201_CREATED)
def create_model(model:Model.Create, session=Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        created_model = Model.create(
            session, 
            model,
            created_by=auth.sub_uuid()
        )
        
        # Send telemetry event
        send_telemetry_event(
            "model_created"
        )
        
        return created_model
    except Model.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{model_id}", response_model=Model.Public)
def update_model(model_id:uuid.UUID, model:Model.Update, session=Depends(get_session), auth:Auth = Security(get_auth)):
    try:
        return Model.update(
            session, 
            model_id,
            model,
            modified_by=auth.sub_uuid()
        )
    except Model.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=List[Model.Public])
def get_models(
        session = Depends(get_session),
        auth:Auth = Security(get_auth),
        offset: int = 0,
        limit: Annotated[int, Query(le=100)] = 100,
):
    try:
        return Model.list(session, offset, limit, auth.org_uuid())
    except Model.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    

@router.get("/{model_id}", response_model=Model.Public)
def read_model(model_id: uuid.UUID, session=Depends(get_session)):
    return get_model_by_id(session, model_id)


@router.delete("/{model_id}")
def delete_model(model_id: uuid.UUID, session=Depends(get_session)):
    try:
        Model.delete(session, model_id)
        return {"ok": True}
    except Model.Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    