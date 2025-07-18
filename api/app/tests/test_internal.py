from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

from app.tests.test_recipients import create_test_recipient
from app.tests.test_models import create_test_model
from app.tests.test_sources import create_test_source
from app.tests.test_destinations import create_test_destination


def test_get_recipient():
    recipient_id = create_test_recipient()
    r = client.get(f"/internal/recipients/{recipient_id}")
    assert r.status_code == 200


def test_get_source():
    source_id = create_test_source()
    r = client.get(f"/internal/sources/{source_id}")
    assert r.status_code == 200

    o = r.json()

    # ensure we're getting sensitive field values
    assert o['connection_info']['access_token'] == 'token'


def test_get_model():
    source_id = create_test_source()
    model_id = create_test_model(source_id)
    r = client.get(f"/internal/models/{model_id}")    
    assert r.status_code == 200


def test_get_destination():
    recipient_id = create_test_recipient()
    source_id = create_test_source()
    model_id = create_test_model(source_id)
    destination_id = create_test_destination(recipient_id, [model_id])

    r = client.get(f"/internal/destinations/{destination_id}")    
    assert r.status_code == 200

    o = r.json()

    # ensure we're getting sensitive field values
    assert o['connection_info']['access_token'] == 'token'


def test_create_transfer_run():

    transfer_id = "1921d9b5-2115-4612-8431-31310e5d7270"

    r = client.post(f"/internal/runs", json={
        "transfer_id": transfer_id,
        "status": "RUNNING",
        "meta": {"arguments": ["a", "b", 3]}
    })

    assert r.status_code == 200

    transfer_run_id = r.json()['transfer_run_id']

    r = client.put(f"/internal/runs/{transfer_run_id}", json={
        "status": "SUCCESS",
        "output": {"records": 1000, "time": 3}
    })
    assert r.status_code == 200

    r = client.get(f"/internal/runs/{transfer_id}")
    assert r.status_code == 200
    o = r.json()
    assert o['status'] == 'SUCCESS'
    assert o['output'] == {"records": 1000, "time": 3}
    assert o['meta'] == {"arguments": ["a", "b", 3]}
    assert o['created_at'] != None