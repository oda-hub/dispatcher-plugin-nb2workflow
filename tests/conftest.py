from cdci_data_analysis.pytest_fixtures import (
            dispatcher_live_fixture, 
            dispatcher_debug,
            dispatcher_test_conf,
            dispatcher_test_conf_fn,
            app
        )
import pytest
import json
import os

config_one_instrument = """    
instruments:
  example0:
    data_server_url: http://localhost:9393
    dummy_cache: ""
"""

@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", 9393)

@pytest.fixture
def mock_backend(httpserver):
    with open('tests/responses/options.json', 'r') as fd:
        respjson = json.loads(fd.read())
    with open('tests/responses/lightcurve.json', 'r') as fd:
        runjson = json.loads(fd.read())
    with open('tests/responses/table.json', 'r') as fd:
        table_json = json.loads(fd.read())
    with open('tests/responses/ascii_binary.json', 'r') as fd:
        bin_json = json.loads(fd.read())
    with open('tests/responses/image.json', 'r') as fd:
        image_json = json.loads(fd.read())
        
    httpserver.expect_request('/').respond_with_data('')    
    httpserver.expect_request(f'/api/v1.0/options').respond_with_json(respjson)
    httpserver.expect_request(f'/api/v1.0/get/lightcurve').respond_with_json(runjson)
    httpserver.expect_request(f'/api/v1.0/get/table').respond_with_json(table_json)
    httpserver.expect_request(f'/api/v1.0/get/ascii_binary').respond_with_json(bin_json)
    httpserver.expect_request(f'/api/v1.0/get/image').respond_with_json(image_json)

@pytest.fixture(scope='session')
def conf_file(tmp_path_factory):
    d = tmp_path_factory.mktemp('nb2wconf')
    fn = d / 'plugin_conf.yml'
    fn.write_text(config_one_instrument)
    yield str(fn.resolve())

# @pytest.fixture
# def dispatcher_plugin_config_env(conf_file, monkeypatch):
#     monkeypatch.setenv('CDCI_NB2W_PLUGIN_CONF_FILE', conf_file)

@pytest.fixture(scope='session', autouse=True)
def set_env_var_plugin_config_file_path(conf_file):
    old_environ = dict(os.environ)
    os.environ['CDCI_NB2W_PLUGIN_CONF_FILE'] = conf_file
    yield
    
    os.environ.clear()
    os.environ.update(old_environ)
    
