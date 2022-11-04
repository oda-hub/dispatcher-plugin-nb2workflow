import pytest
import json
import logging
import requests
import os
#from dispatcher_plugin_nb2workflow import conf_file


logger = logging.getLogger(__name__)

config_one_instrument = """    
instruments:
  example0:
    data_server_url: http://localhost:9393
    dummy_cache: ""
"""

config_two_instruments = """    
instruments:
  example0:
    data_server_url: http://localhost:9393
    dummy_cache: ""
  example1:
    data_server_url: http://localhost:9494
    dummy_cache: ""
"""

src_arguments = [
  "src_name",
  "RA",
  "DEC",
  "T1",
  "T2",
  "T_format",
  "token",
]
expected_arguments = src_arguments + ['seed']
    
@pytest.fixture
def conf_file(tmp_path):
    d = tmp_path
    d.mkdir(exist_ok=True)
    fn = d / 'plugin_conf.yml'
    fn.write_text(config_one_instrument)
    yield str(fn.resolve())

@pytest.fixture
def dispatcher_plugin_config_env(conf_file, monkeypatch):
    monkeypatch.setenv('CDCI_NB2W_PLUGIN_CONF_FILE', conf_file)

@pytest.fixture
def mock_backend(httpserver):
    with open('tests/responses/options.json', 'r') as fd:
        respjson = json.loads(fd.read())
        
    httpserver.expect_request('/').respond_with_data('')    
    httpserver.expect_request(f'/api/v1.0/options').respond_with_json(respjson)
    

def test_discover_plugin():
    import cdci_data_analysis.plugins.importer as importer

    assert 'dispatcher_plugin_nb2workflow' in  importer.cdci_plugins_dict.keys()
    
def test_instrument_available(dispatcher_plugin_config_env, dispatcher_live_fixture, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
   
    c = requests.get(server + "/api/instr-list",
                    params = {'instrument': 'mock'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert 'example0' in jdata

def test_instrument_parameters(dispatcher_plugin_config_env, dispatcher_live_fixture, mock_backend, caplog):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
       
    c = requests.get(server + "/api/par-names",
                    params = {'instrument': 'example0'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert sorted(jdata) == sorted(expected_arguments) or sorted(jdata) == sorted(expected_arguments[:5]+expected_arguments[6:]) # TODO: leave one option
    assert "will be discarded for the instantiation" not in caplog.text
    assert "Possibly a programming error" not in caplog.text

def test_instrument_products(dispatcher_plugin_config_env, dispatcher_live_fixture, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
       
    c = requests.get(server + "/api/meta-data",
                    params = {'instrument': 'example0'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    for elem in jdata[0]:
        if isinstance(elem, dict) and 'prod_dict' in elem.keys():
            prod_dict = elem['prod_dict']
    assert prod_dict == {'lightcurve': 'lightcurve_query'}

def test_instrument_backend_unavailable(dispatcher_plugin_config_env, dispatcher_live_fixture):
    # current behaviour is to have instrument with no products, could be changed in the future
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
       
    c = requests.get(server + "/api/meta-data",
                    params = {'instrument': 'example0'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    for elem in jdata[0]:
        if isinstance(elem, dict) and 'prod_dict' in elem.keys():
            prod_dict = elem['prod_dict']
    assert prod_dict == {}

def test_instrument_added(conf_file, dispatcher_plugin_config_env, dispatcher_live_fixture, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
    
    c = requests.get(server + "/api/instr-list",
                    params = {'instrument': 'mock'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert 'example1' not in jdata
       
    with open(conf_file, 'w') as fd:
        fd.write(config_two_instruments)
    c = requests.get(server + "/reload-plugin/dispatcher_plugin_nb2workflow")
    assert c.status_code == 200
    
    c = requests.get(server + "/api/instr-list",
                    params = {'instrument': 'mock'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert 'example0' in jdata
    assert 'example1' in jdata    
    