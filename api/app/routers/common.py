
import uuid
from fastapi import HTTPException
from app.models import Auth, Task, TransferRun


def create_transfer_task(session, check_type:str, transfer_id:str, auth:Auth, status='RUNNING', output={}, meta={}) -> Task.Model:
    """ Create an async task to monitor a transfer job """
    
    task = Task.create(
        session, 
        Task.Create.model_validate({
            "status": status,
            "output": output,
            "meta": {
                "type": check_type, 
                "transfer_id": transfer_id
            } | meta
        }), 
        auth.sub_uuid(), 
        auth.org_uuid()
    )

    return task


def transfer_task_status(session, task_id:uuid.UUID, timeout:int=300) -> Task.Model:
    """ Check the status of an async task monitoring a transfer job """

    task = Task.get(session, task_id)
    if task is None:
        raise HTTPException(status_code=400, detail="Task not found")

    if task.status == 'COMPLETE':
        return task

    transfer_id = task.meta.get('transfer_id')
    if transfer_id is None:
        raise HTTPException(status_code=400, detail="Invalid task")
        
    transfer_run = TransferRun.get_latest_transfer_run(session, transfer_id)
    if transfer_run is None:
        return task
    
    if transfer_run.status == 'RUNNING':
        return task
    
    task.output = transfer_run.output

    if 'success' not in task.output:
        task.output['success'] = transfer_run.status == 'SUCCESS'

    task.status = 'COMPLETE'
    Task.update(session, task_id, task)
    
    return task
