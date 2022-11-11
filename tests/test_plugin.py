import json
import logging
import requests

logger = logging.getLogger(__name__)

config_two_instruments = """    
instruments:
  example0:
    data_server_url: http://localhost:9393
    dummy_cache: ""
  example1:
    data_server_url: http://localhost:9494
    dummy_cache: ""
"""

expected_arguments = ["T1",
                      "T2",
                      "T_format",
                      "token",
                      "seed"]
    
def test_discover_plugin():
    import cdci_data_analysis.plugins.importer as importer

    assert 'dispatcher_plugin_nb2workflow' in  importer.cdci_plugins_dict.keys()
    
def test_instrument_available(dispatcher_live_fixture, mock_backend):
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

def test_instrument_parameters(dispatcher_live_fixture, caplog, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
       
    c = requests.get(server + "/api/par-names",
                    params = {'instrument': 'example0'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert sorted(jdata) == sorted(expected_arguments)
    assert "will be discarded for the instantiation" not in caplog.text
    assert "Possibly a programming error" not in caplog.text
    
def test_default_src_par_value(dispatcher_live_fixture, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
       
    c = requests.get(server + "/meta-data",
                    params = {'instrument': 'example0'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    src_query_descr = json.loads([x for x in jdata[0] if "src_query" in x][0])
    T1_value = [x for x in src_query_descr if x.get('name') == 'T1'][0]['value']
    assert T1_value == '2021-06-25T05:59:37.000'

def test_instrument_products(dispatcher_live_fixture, mock_backend):
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

def test_instrument_backend_unavailable(dispatcher_live_fixture):
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

def test_instrument_added(conf_file, dispatcher_live_fixture, mock_backend):
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
    
def test_pass_comment(dispatcher_live_fixture, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
       
    c = requests.get(server + "/run_analysis",
                    params = {'instrument': 'example0',
                              'query_status': 'new',
                              'query_type': 'Real',
                              'product_type': 'lightcurve',
                              'api': 'True'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert 'TEST COMMENT' in jdata['exit_status']['comment']
    
