
from app.tests.common import AuthTestClient
from app.main import app

client = AuthTestClient(app)

def create_test_recipient():
    r = client.post("/recipients", json={
        "recipient_name": "Test Company, Inc",
        "tenant_id": "CustomerABC123"
    })
    assert r.status_code == 201
    return r.json()['recipient_id']


def test_get_recipients():
    r = client.get("/recipients")
    assert r.status_code == 200


def test_create_recipient():

    r = client.post("/recipients", json={
        "recipient_name": "Test Company, Inc",
        "tenant_id": "CustomerABC123"
    })
    assert r.status_code == 201
    
    o = r.json()
    assert set(o.keys()) == set([
        'recipient_id', 
        'recipient_name', 
        'tenant_id', 
        'created_at', 
        'modified_at', 
    ])

    assert o['recipient_name'] == "Test Company, Inc"
    assert o['tenant_id'] == "CustomerABC123"


def test_update_recipient():
    recipient_id = create_test_recipient()
    r = client.put(f"/recipients/{recipient_id}", json={
        "recipient_name": "Acme, Inc.",
        "tenant_id": "Acme, Inc."
    })
    assert r.status_code == 200
    o = r.json()
    assert set(o.keys()) == set([
        'recipient_id', 
        'recipient_name', 
        'tenant_id', 
        'created_at', 
        'modified_at', 
    ])
    assert o['recipient_name'] == "Acme, Inc."
    assert o['tenant_id'] == "Acme, Inc."


def test_get_recipient():
    recipient_id = create_test_recipient()
    r = client.get(f"/recipients/{recipient_id}")
    o = r.json()
    assert set(o.keys()) == set([
        'recipient_id', 
        'recipient_name', 
        'tenant_id', 
        'created_at', 
        'modified_at', 
    ])

    assert o['recipient_id'] == recipient_id
    assert o['recipient_name'] == "Test Company, Inc"
    assert o['tenant_id'] == "CustomerABC123"


def test_delete_recipient():
    recipient_id = create_test_recipient()
    r = client.delete(f"/recipients/{recipient_id}")
    assert r.status_code == 200
    assert r.json() == {'ok': True}

