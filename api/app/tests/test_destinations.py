from app.tests.common import AuthTestClient

from app.main import app

client = AuthTestClient(app)

from app.tests.test_recipients import create_test_recipient
from app.tests.test_models import create_test_model
from app.tests.test_sources import create_test_source


def create_test_destination(recipient_id, model_ids):
    r = client.post("/destinations", json={
        "destination_name": "Test",
        "recipient_id": recipient_id,
        "schedule": {"type": "FULL_REFRESH", "frequency": "DAILY"},
        "models": model_ids,
        "vendor_type": "snowflake",
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin",
                "account": "abc123",
                "access_token": "token",
                "warehouse": "primary",
                "database": "mydb",
                "target_schema": "export"
            }
    })
    assert r.status_code == 201
    return r.json()['destination_id']


def test_get_destinations():
    r = client.get("/destinations")
    assert r.status_code == 200


def test_create_destination():

    source_id = create_test_source()
    recipient_id = create_test_recipient()
    model_id = create_test_model(source_id)


    r = client.post("/destinations", json={
        "destination_name": "Test",
        "recipient_id": recipient_id,
        "schedule": {"type": "FULL_REFRESH", "frequency": "DAILY"},
        "models": [model_id],
        "vendor_type": "redshift",
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin",
                "account": "abc123",
                "access_token": "token",
                "warehouse": "primary",
                "database": "mydb",
                "target_schema": "export"
            }
    })

    # mismatch vendor types
    assert r.status_code == 400

    r = client.post("/destinations", json={
        "destination_name": "Test",
        "recipient_id": recipient_id,
        "schedule": {"type": "FULL_REFRESH", "frequency": "DAILY"},
        "models": [],
        "vendor_type": "snowflake",
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin",
                "account": "abc123",
                "access_token": "token",
                "warehouse": "primary",
                "database": "mydb",
                "target_schema": "export"
            }
    })

    # empty models
    assert r.status_code == 400

    r = client.post("/destinations", json={
        "destination_name": "Test",
        "recipient_id": recipient_id,
        "schedule": {"type": "FULL_REFRESH", "frequency": "DAILY"},
        "models": [model_id],
        "vendor_type": "snowflake"
    })

    # missing connection info field
    assert r.status_code == 422

    r = client.post("/destinations", json={
        "destination_name": "Test",
        "recipient_id": recipient_id,
        "schedule": {"type": "FULL_REFRESH", "frequency": "DAILY"},
        "models": [model_id],
        "vendor_type": "snowflake",
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin",
                "account": "abc123",
                "access_token": "token",
                "warehouse": "primary",
                "database": "mydb",
                "target_schema": "export"
            }
    })

    # should work
    assert r.status_code == 201
    assert set(r.json().keys()) == set([
        'destination_id', 
        'destination_name',
        'schedule',
        'models',
        'recipient_id',
        'vendor_type', 
        'state',
        'is_enabled', 
        'created_at', 
        'modified_at'
    ])


def test_update_destination():
    
    source_id = create_test_source()
    recipient_id = create_test_recipient()
    model_id = create_test_model(source_id)
    destination_id = create_test_destination(recipient_id, [model_id])

    r = client.put(f"/destinations/{destination_id}", json={
        "destination_name": "Test2",            # update
        "is_enabled": True,                     # enable
        "state": "CREATED",                     # this will create scheduled Tranfer
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin2",               # update
                "account": "abc123",
                "access_token": "mynewtoken",        # update
                "warehouse": "primary",
                "database": "mydb",
                "target_schema": "export"
            }
    })

    assert r.status_code == 200
    
    o = r.json()
    assert set(o.keys()) == set([
        'destination_id', 
        'destination_name',
        'schedule',
        'models',
        'recipient_id',
        'primary_transfer_id',
        'vendor_type', 
        'connection_info',
        'state',
        'is_enabled', 
        'created_at', 
        'modified_at'
    ])

    assert o['destination_id'] == destination_id
    assert o['destination_name'] == 'Test2'
    assert o['state'] == 'CREATED'
    assert o['is_enabled'] == True
    assert o['primary_transfer_id'] != None
    assert o['connection_info']['user'] == 'admin2'
    assert o['connection_info']['access_token'] == '****'

    # change schedule and disable (will update Transfer schedule and disable it)
    r = client.put(f"/destinations/{destination_id}", json={
        "schedule": {"type": "INCREMENTAL", "frequency": "WEEKLY", "day": 2},
        "is_enabled": False           
    })

    assert r.status_code == 200

    # this will delete corresponding Transfer as well
    r = client.delete(f"/destinations/{destination_id}")
    assert r.status_code == 200
    assert r.json() == {'ok': True}


def test_clone_destination():

    source_id = create_test_source()
    recipient_id = create_test_recipient()
    model_id = create_test_model(source_id)
    destination_id = create_test_destination(recipient_id, [model_id])

    r = client.put(f"/destinations/{destination_id}", json={
        "state": "CREATED",
        "is_enabled": True   
    })
    assert r.status_code == 200

    r = client.post(f"/destinations/{destination_id}/clone")
    assert r.status_code == 201

    o = r.json()
    clone_destination_id = o['destination_id']
    assert clone_destination_id != destination_id
    assert o['state'] == 'DRAFT' 

    r = client.delete(f"/destinations/{destination_id}")
    assert r.status_code == 200
    assert r.json() == {'ok': True}

    r = client.delete(f"/destinations/{clone_destination_id}")
    assert r.status_code == 200
    assert r.json() == {'ok': True}


def test_get_destination():
    
    source_id = create_test_source()
    recipient_id = create_test_recipient()
    model_id = create_test_model(source_id)
    destination_id = create_test_destination(recipient_id, [model_id])

    r = client.get(f"/destinations/{destination_id}")
    assert r.status_code == 200
    o = r.json()

    assert set(o.keys()) == set([
        'destination_id', 
        'destination_name',
        'schedule',
        'models',
        'recipient_id',
        'primary_transfer_id',
        'vendor_type', 
        'connection_info',
        'state',
        'is_enabled', 
        'created_at', 
        'modified_at'
    ])

    assert o['destination_id'] == destination_id
    assert o['destination_name'] == 'Test'
    assert o['models'] == [model_id]
    assert o['recipient_id'] == recipient_id
    assert o['state'] == 'DRAFT'
    assert o['connection_info']['user'] == 'admin'
    assert o['connection_info']['access_token'] == '****'


def test_delete_destination():

    source_id = create_test_source()
    recipient_id = create_test_recipient()
    model_id = create_test_model(source_id)
    destination_id = create_test_destination(recipient_id, [model_id])

    r = client.delete(f"/destinations/{destination_id}")
    assert r.status_code == 200
    assert r.json() == {'ok': True}


def test_destination_check():
    
    destination_id = 'df2267d6-fe65-43bb-b9ad-0c23b23a2c35'
    r = client.post(f"/destinations/{destination_id}/check")
    assert r.status_code == 200
    o = r.json()

    assert 'task_id' in o
    task_id = o['task_id']

    r = client.get(f"/destinations/{destination_id}/check/{task_id}")
    assert r.status_code == 200
    o = r.json()

    assert o['status'] == 'COMPLETE'
    assert o['output']['success'] == True

