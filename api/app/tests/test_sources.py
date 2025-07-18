from app.tests.common import AuthTestClient
from app.main import app

client = AuthTestClient(app)


def create_test_source():
    r = client.post("/sources", json={
        "source_name": "Test",
        "vendor_type": "snowflake",
        "is_enabled": True,
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin",
                "account": "abc123",
                "access_token": "token",
                "database": "mydb",
                "warehouse": "primary"
            }
    })
    assert r.status_code == 201
    return r.json()['source_id']


def test_get_sources():
    r = client.get("/sources")
    assert r.status_code == 200


def test_create_source():
    r = client.post("/sources/", json={
        "source_name": "Test",
        "vendor_type": "redshift",
        "is_enabled": True,
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin",
                "account": "abc123",
                "access_token": "token",
                "warehouse": "primary",
                "database": "mydb"
            }
    })

    # mismatch vendor types
    assert r.status_code == 400

    r = client.post("/sources/", json={
        "source_name": "Test",
        "vendor_type": "snowflake",
        "is_enabled": True,
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin",
                "database": "mydb",
                #"account": "abc123",
                "access_token": "token",
                "warehouse": "primary"
            }
    })

    # missing account info field
    assert r.status_code == 422

    r = client.post("/sources/", json={
        "source_name": "Test",
        "vendor_type": "snowflake",
        "is_enabled": True,
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin",
                "account": "abc123",
                "access_token": "token",
                "warehouse": "primary",
                "database": "mydb"
            }
    })

    # should work
    assert r.status_code == 201
    assert set(r.json().keys()) == set([
        'source_id', 
        'source_name', 
        'vendor_type',
        'state',
        'is_enabled', 
        'created_at', 
        'modified_at'
    ])


def test_update_source():
    
    source_id = create_test_source()

    r = client.put(f"/sources/{source_id}", json={
        "source_name": "Test2",             # update
        "is_enabled": False,
        "state": "CREATED",
        "connection_info": {
                "vendor_type": "snowflake",
                "user": "admin2",           # update
                "account": "abc123",
                "access_token": "mynewtoken",    # update
                "warehouse": "primary",
                "database": "mydb"
            }
    })

    assert r.status_code == 200
    
    o = r.json()
    assert set(o.keys()) == set([
        'source_id', 
        'source_name', 
        'vendor_type', 
        'state',
        'is_enabled', 
        'created_at', 
        'modified_at',
        'connection_info'
    ])

    assert o['source_id'] == source_id
    assert o['source_name'] == 'Test2'
    assert o['state'] == 'CREATED'
    assert o['is_enabled'] == False
    assert o['connection_info']['user'] == 'admin2'
    assert o['connection_info']['access_token'] == '****'


def test_get_source():
    
    source_id = create_test_source()

    r = client.get(f"/sources/{source_id}")
    assert r.status_code == 200
    o = r.json()

    assert set(o.keys()) == set([
        'source_id', 
        'source_name', 
        'vendor_type', 
        'state',
        'is_enabled', 
        'created_at', 
        'modified_at',
        'connection_info'
    ])

    assert o['source_id'] == source_id
    assert o['source_name'] == 'Test'
    assert o['state'] == 'DRAFT'
    assert o['is_enabled'] == True
    assert o['connection_info']['user'] == 'admin'
    assert o['connection_info']['access_token'] == '****'
    assert o['connection_info']['warehouse'] == 'primary'


def test_clone_source():
    source_id = create_test_source()
    r = client.put(f"/sources/{source_id}", json={
        "state": "CREATED",
        "is_enabled": True
    })
    assert r.status_code == 200

    r = client.post(f"/sources/{source_id}/clone")
    assert r.status_code == 201
    o = r.json()

    clone_source_id = o['source_id']
    assert clone_source_id != source_id
    assert o['state'] == 'DRAFT'
    assert o['source_name'] == 'Test'


def test_delete_source():
    source_id = create_test_source()
    r = client.delete(f"/sources/{source_id}")
    assert r.status_code == 200
    assert r.json() == {'ok': True}



def test_source_check():
    source_id = '8f491f6e-1883-43d4-8189-bc9db7276c43'
    r = client.post(f"/sources/{source_id}/check")
    assert r.status_code == 200
    o = r.json()

    assert 'task_id' in o
    task_id = o['task_id']

    r = client.get(f"/sources/{source_id}/check/{task_id}")
    assert r.status_code == 200
    o = r.json()

    assert o['status'] == 'COMPLETE'
    assert o['output']['success'] == True



def test_source_metadata():
    source_id = '8f491f6e-1883-43d4-8189-bc9db7276c43'
    r = client.post(f"/sources/{source_id}/metadata")
    assert r.status_code == 200
    o = r.json()

    assert 'task_id' in o
    task_id = o['task_id']

    r = client.get(f"/sources/{source_id}/metadata/{task_id}")
    assert r.status_code == 200
    o = r.json()

    assert o['status'] == 'COMPLETE'
    assert o['output']['success'] == True
    assert isinstance(o['output']['stream_info'], dict)
    assert isinstance(o['output']['stream_info']['streams'], list)
