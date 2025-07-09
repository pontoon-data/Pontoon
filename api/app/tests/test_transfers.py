from fastapi.testclient import TestClient
from app.tests.common import AuthTestClient
from app.main import app


from app.tests.test_recipients import create_test_recipient
from app.tests.test_models import create_test_model
from app.tests.test_sources import create_test_source
from app.tests.test_destinations import create_test_destination


# /internal does not use Bearer auth
internal = TestClient(app)

# regular auth client
client = AuthTestClient(app)


def test_list_transfers():

    # create a destination
    source_id = create_test_source()
    recipient_id = create_test_recipient()
    model_id = create_test_model(source_id)
    destination_id = create_test_destination(recipient_id, [model_id])

    # Note: this is where you would normally do a destination /check before enabling ...

    # enable destination to create a transfer entry
    r = client.put(f"/destinations/{destination_id}", json={
        "is_enabled": True,                     # enable
        "state": "CREATED"                      # triggers schedule Transfer creation
    })
    assert r.status_code == 200

    # this is our scheduled primary transfer
    transfer_id = r.json()['primary_transfer_id']

    # create a transfer run via /internal
    r = internal.post(f"/internal/runs", json={
        "transfer_id": transfer_id,
        "status": "RUNNING",
        "meta": {}
    })

    assert r.status_code == 200
    transfer_run_id = r.json()['transfer_run_id']

    # list the destination transfer runs to verify status
    r = client.get(f"/transfers", params={"destination_id": destination_id})
    assert r.status_code == 200
    o = r.json()
    assert isinstance(o, list)
    assert len(o) == 1
    run = o[0]
    assert run['transfer_id'] == transfer_id
    assert run['transfer_run_id'] == transfer_run_id
    assert run['status'] == 'RUNNING'

    # update run status via /internal
    r = internal.put(f"/internal/runs/{transfer_run_id}", json={
        "status": "SUCCESS",
        "output": {"records": 1000, "time": 3}
    })
    assert r.status_code == 200

    # list again to verify status change
    r = client.get(f"/transfers", params={"destination_id": destination_id})
    assert r.status_code == 200
    run = r.json()[0]
    assert run['transfer_id'] == transfer_id
    assert run['transfer_run_id'] == transfer_run_id
    assert run['status'] == 'SUCCESS'
    assert run['output'] == {"records": 1000, "time": 3}

    # delete destination to cleanup any transfer infra created
    r = client.delete(f"/destinations/{destination_id}")
    assert r.status_code == 200
