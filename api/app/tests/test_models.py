from app.tests.common import AuthTestClient

from app.main import app
from app.tests.test_sources import create_test_source

client = AuthTestClient(app)

def create_test_model(source_id):
    r = client.post("/models", json={
        "source_id": source_id,
        "model_name": "Test",
        "model_description": "Test Model",
        "schema_name": "pontoon",
        "table_name": "test",
        "include_columns": [],
        "primary_key_column": "id",
        "tenant_id_column": "customer_id",
        "last_modified_at_column": "last_modified"
    })
    assert r.status_code == 201
    return r.json()['model_id']


def test_get_models():
    r = client.get("/models")
    assert r.status_code == 200


def test_create_model():

    r = client.post("/models", json={
        "source_id": "f71f2bfc-8761-462b-bd30-9b0f77d67d8e", # does not exist
        "model_name": "Test",
        "model_description": "Test Model",
        "schema_name": "pontoon",
        "table_name": "test",
        "include_columns": [],
        "primary_key_column": "id",
        "tenant_id_column": "customer_id",
        "last_modified_at_column": "last_modified"
    })
    # source does not exist
    assert r.status_code == 400

    r = client.post("/models", json={
        #"source_id": "f71f2bfc-8761-462b-bd30-9b0f77d67d8e",
        "model_name": "Test",
        "model_description": "Test Model",
        "schema_name": "pontoon",
        "table_name": "test",
        "include_columns": [],
        "primary_key_column": "id",
        "tenant_id_column": "customer_id",
        "last_modified_at_column": "last_modified"
    })
    # missing required field
    assert r.status_code == 422

    source_id = create_test_source()
    r = client.post("/models", json={
        "source_id": source_id, 
        "model_name": "Test",
        "model_description": "Test Model",
        "schema_name": "pontoon",
        "table_name": "test",
        "include_columns": [],
        "primary_key_column": "id",
        "tenant_id_column": "customer_id",
        "last_modified_at_column": "last_modified"
    })
    # this one should work
    assert r.status_code == 201
    
    o = r.json()
    assert set(o.keys()) == set([
        'model_id',
        'source_id', 
        'model_name',
        'model_description',
        'schema_name', 
        'table_name',
        'include_columns',
        'primary_key_column',
        'tenant_id_column',
        'last_modified_at_column', 
        'created_at', 
        'modified_at' 
    ])

    assert o['source_id'] == source_id
    assert o['model_name'] == "Test"
    assert o['schema_name'] == "pontoon"
    assert o['table_name'] == "test"
    assert o['include_columns'] == []
    assert o['primary_key_column'] == "id"
    assert o['tenant_id_column'] == "customer_id"
    assert o['last_modified_at_column'] == "last_modified"


def test_update_model():
    source_id = create_test_source()
    model_id = create_test_model(source_id)
    
    r = client.put(f"/models/{model_id}", json={
        "schema_name": "pontoon2",
        "table_name": "test2",
        "include_columns": [{"name": "howdy"}],
        "primary_key_column": "id2",
        "tenant_id_column": "customer_id2",
        "last_modified_at_column": "last_modified2"
    })

    assert r.status_code == 200
    
    o = r.json()
    assert set(o.keys()) == set([
        'model_id',
        'source_id', 
        'model_name',
        'model_description',
        'schema_name', 
        'table_name',
        'include_columns',
        'primary_key_column',
        'tenant_id_column',
        'last_modified_at_column', 
        'created_at', 
        'modified_at' 
    ])
    
    assert o['source_id'] == source_id
    assert o['schema_name'] == "pontoon2"
    assert o['table_name'] == "test2"
    assert o['primary_key_column'] == "id2"
    assert o['tenant_id_column'] == "customer_id2"
    assert o['include_columns'] == [{"name": "howdy"}]
    assert o['last_modified_at_column'] == "last_modified2"


def test_get_model():
    source_id = create_test_source()
    model_id = create_test_model(source_id)
    
    r = client.get(f"/models/{model_id}")
    assert r.status_code == 200

    o = r.json()
    assert set(o.keys()) == set([
        'model_id',
        'source_id', 
        'model_name',
        'model_description',
        'schema_name', 
        'table_name',
        'include_columns',
        'primary_key_column',
        'tenant_id_column',
        'last_modified_at_column', 
        'created_at', 
        'modified_at'
    ])

    assert o['source_id'] == source_id


def test_delete_model():
    source_id = create_test_source()
    model_id = create_test_model(source_id)
    r = client.delete(f"/models/{model_id}")
    assert r.status_code == 200
    assert r.json() == {'ok': True}

