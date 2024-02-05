import json
import logging
import requests
from oda_api.data_products import PictureProduct, ImageDataProduct
import time
import jwt
import pytest
from oda_api.api import RequestNotUnderstood
import re
import gzip
import os
from magic import from_buffer as mime_from_buffer

logger = logging.getLogger(__name__)

config_two_instruments = """    
instruments:
  example0:
    data_server_url: http://localhost:9494
    dummy_cache: ""
  example1:
    data_server_url: http://localhost:9595
    dummy_cache: ""
"""

config_local_kg = """
kg:
  type: "file"
  path: "tests/example-kg.ttl"
"""

config_real_nb2service = """
instruments:
  example:
    data_server_url: %s
    dummy_cache: ""
    restricted_access: false
"""

expected_arguments = ["T1",
                      "T2",
                      "T_format",
                      "token",
                      "seed",
                      "some_param"]

secret_key = 'secretkey_test'
default_exp_time = int(time.time()) + 5000

token_payload = {'sub': "user@example.com",
                         'name': "username",
                         'roles': "oda workflow developer",
                         'exp': default_exp_time,
                         'tem': 0}
encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

def sorted_items(dic):
    return sorted(dic.items(), key = lambda x: x[0])

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
    assert prod_dict == {'ascii_binary': 'ascii_binary_query',
                         'image': 'image_query',
                         'lightcurve': 'lightcurve_query',
                         'table': 'table_query'}

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
                              'api': 'True',
                              'run_asynch': 'False'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert 'TEST COMMENT' in jdata['exit_status']['comment']
    

def test_table_product(dispatcher_live_fixture, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
    
    with open('tests/responses/table.json', 'r') as fd:
        tab_resp_json = json.loads(fd.read())
        ascii_rep = tab_resp_json['output']['output']['ascii']

    c = requests.get(server + "/run_analysis",
                    params = {'instrument': 'example0',
                              'query_status': 'new',
                              'query_type': 'Real',
                              'product_type': 'table',
                              'api': 'True',
                              'run_asynch': 'False'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert jdata['products']['astropy_table_product_ascii_list'][0]['ascii'] == ascii_rep

def test_text_product(dispatcher_live_fixture, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    with open('tests/responses/ascii_binary.json', 'r') as fd:
        tab_resp_json = json.loads(fd.read())
        ascii_rep = tab_resp_json['output']['text_output']

    c = requests.get(server + "/run_analysis",
                    params = {'instrument': 'example0',
                              'query_status': 'new',
                              'query_type': 'Real',
                              'product_type': 'ascii_binary',
                              'api': 'True',
                              'run_asynch': 'False'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert jdata['products']['text_product_list'][0]['name'] == 'text_output'
    assert jdata['products']['text_product_list'][0]['value'] == ascii_rep
    
def test_binimage_product(dispatcher_live_fixture, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    c = requests.get(server + "/run_analysis",
                    params = {'instrument': 'example0',
                              'query_status': 'new',
                              'query_type': 'Real',
                              'product_type': 'ascii_binary',
                              'api': 'True',
                              'run_asynch': 'False'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    imdata = jdata['products']['binary_image_product_list'][0]
    oda_dp = PictureProduct.decode(imdata)
    assert oda_dp.img_type == 'png'
    
def test_image_product(dispatcher_live_fixture, mock_backend):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    c = requests.get(server + "/run_analysis",
                    params = {'instrument': 'example0',
                              'query_status': 'new',
                              'query_type': 'Real',
                              'product_type': 'image',
                              'api': 'True',
                              'run_asynch': 'False'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    imdata = jdata['products']['numpy_data_product_list'][0]
    oda_ndp = ImageDataProduct.decode(imdata)

def test_default_kg(dispatcher_live_fixture):  
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    c = requests.get(server + "/instr-list",
                    params = {'instrument': 'mock', 
                              'token': encoded_token})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200 
    assert 'lightcurve-example' in jdata # TODO: change to what will be used in docs

@pytest.mark.parametrize("privileged", [True, False])
def test_local_kg(conf_file, dispatcher_live_fixture, privileged):  
    with open(conf_file, 'r') as fd:
        conf_bk = fd.read()
        
    try:
        with open(conf_file, 'w') as fd:
            fd.write(config_local_kg)
            
        server = dispatcher_live_fixture
        logger.info("constructed server: %s", server)

        c = requests.get(server + "/reload-plugin/dispatcher_plugin_nb2workflow")
        assert c.status_code == 200 
        
        params = {'instrument': 'mock'}
        if privileged:
            params['token'] = encoded_token
            
        c = requests.get(server + "/instr-list",
                        params = params)
        logger.info("content: %s", c.text)
        jdata = c.json()
        logger.info(json.dumps(jdata, indent=4, sort_keys=True))
        logger.info(jdata)
        assert c.status_code == 200
        assert 'kgprod' in jdata
        if privileged:
            assert 'kgexample' in jdata
            assert 'kgunlab' in jdata
        else:
            assert 'kgexample' not in jdata
            assert 'kgunlab' not in jdata
    finally:
        with open(conf_file, 'w') as fd:
            fd.write(conf_bk)        
# TODO: test unreachable kb


# apart from general end-to-end testing this also tests for issue #47 
@pytest.mark.fullstack
@pytest.mark.parametrize("set_param, expect_param, wrong",
                         [({}, {}, False),
                          
                          ({'T_format': 'isot',
                            'time_instant': '2019-09-19T12:00:00.000',
                            'T1': '2019-09-19T12:00:00.000',
                            'T2': '2019-09-20T12:00:00.000'}, 
                           {'time_instant': '2019-09-19T12:00:00.000', 
                            'start_time': 58745.5, 'end_time': 58746.5}, False),
                          
                          ({'T_format': 'mjd', 'time_instant': 58745.5, 'T2': 58746.5}, 
                           {'time_instant': '2019-09-19T12:00:00.000', 
                            'end_time': 58746.5}, False),
                          
                          ({'RA': 23.5, 'DEC': 33.3}, {'poi_ra': 23.5, 'poi_dec': 33.3}, False),
                          
                          ({'energy': 500, 'band': 'b', 'radius': 1.2}, 
                           {'energy': 500, 'band': 'b', 'radius': 1.2}, False),
                          
                          ({'visible_band': 'z'}, {'visible_band': 'z'}, True),
                          
                          ({'energy': 1000}, {'energy': 1000}, True)
                          ])
def test_full_stack(live_nb2service,
                    conf_file, 
                    dispatcher_live_fixture,  
                    set_param, 
                    expect_param,
                    wrong):
    with open(conf_file, 'r') as fd:
        conf_bk = fd.read()
      
    try:
        with open(conf_file, 'w') as fd:
            fd.write( config_real_nb2service % live_nb2service )
        
        server = dispatcher_live_fixture
        logger.info("constructed server: %s", server)    

        #ensure new conf file readed 
        c = requests.get(server + "/reload-plugin/dispatcher_plugin_nb2workflow")
        assert c.status_code == 200 
        
        default_in_params = {'band': 'z', 
                          'end_time': 56005.0, 
                          'energy': 50.0, 
                          'poi_dec': 20.0, 
                          'poi_ra': 10.0, 
                          'radius': 3.0, 
                          'start_time': 56000.0, 
                          'time_instant': '2017-08-17T12:43:00.000', 
                          'visible_band': 'v'}
        request_params = {}
        expected_params = default_in_params.copy()
        
        for k, v in set_param.items():
            request_params[k] = v
        for k, v in expect_param.items():
            expected_params[k] = v
        
        from oda_api.api import DispatcherAPI
        disp = DispatcherAPI(url=server)
        disp.timeout = 300
        if wrong:
            with pytest.raises(RequestNotUnderstood):
                disp.get_product(instrument = "example",
                                 product = "echo",
                                 **request_params)
        else:
            res = disp.get_product(instrument = "example",
                            product = "echo",
                            **request_params)
            
            assert sorted_items(eval(res.output_0.value)) == sorted_items(expected_params) 
        
    finally:
        with open(conf_file, 'w') as fd:
            fd.write(conf_bk)
            
            
@pytest.mark.fullstack
def test_parameter_output(live_nb2service,
                          conf_file, 
                          dispatcher_live_fixture):
    with open(conf_file, 'r') as fd:
        conf_bk = fd.read()
      
    try:
        with open(conf_file, 'w') as fd:
            fd.write( config_real_nb2service % live_nb2service )
        
        server = dispatcher_live_fixture
        logger.info("constructed server: %s", server)    

        #ensure new conf file readed 
        c = requests.get(server + "/reload-plugin/dispatcher_plugin_nb2workflow")
        assert c.status_code == 200 
        
        from oda_api.api import DispatcherAPI
        disp = DispatcherAPI(url=server)
        prod = disp.get_product(instrument = "example", product = "paramout")
        
        names = [x['prod_name'] for x in prod.as_list()]
        restup = [(getattr(prod, x).name, getattr(prod, x).value, getattr(prod, x).meta_data['uri']) for x in names]
        
        assert restup == [('flout', 4.2, 'http://odahub.io/ontology#Float'),
                          ('intout', 4, 'http://odahub.io/ontology#Integer'),
                          ('mrk', 'Mrk 421', 'http://odahub.io/ontology#AstrophysicalObject'),
                          ('timeinst', 56457.0, 'http://odahub.io/ontology#TimeInstantMJD'),
                          ('timeisot',
                          '2022-10-09T13:00:00.000',
                          'http://odahub.io/ontology#TimeInstantISOT'),
                          ('wrng', 'FOO', 'http://odahub.io/ontology#PhotometricBand')]
        
    finally:
        with open(conf_file, 'w') as fd:
            fd.write(conf_bk)

@pytest.mark.fullstack
def test_failed_nbhtml_download(live_nb2service, 
                                conf_file, 
                                dispatcher_live_fixture):
    with open(conf_file, 'r') as fd:
        conf_bk = fd.read()
      
    try:
        with open(conf_file, 'w') as fd:
            fd.write( config_real_nb2service % live_nb2service )
        
        server = dispatcher_live_fixture
        logger.info("constructed server: %s", server)    

        #ensure new conf file readed 
        c = requests.get(server + "/reload-plugin/dispatcher_plugin_nb2workflow")
        assert c.status_code == 200 
        
        for i in range(10):
            c = requests.get(server + "/run_analysis",
                             params = {'instrument': 'example',
                                       'query_status': 'new',
                                       'query_type': 'Real',
                                       'product_type': 'failing',
                                      })
            assert c.status_code == 200
            jdata = c.json()        
            if jdata['job_status'] == 'failed':
                break
            time.sleep(10) 

        assert 'download_products' in jdata['exit_status']['message']        
        downlink = re.search('href="([^\"]*)\"',
                             jdata['exit_status']['message']).groups()[-1]
        
        c = requests.get(downlink)
        assert c.status_code == 200

        htmlcont = gzip.decompress(c.content)
        assert mime_from_buffer(htmlcont, mime=True) == 'text/html'
        assert 'body class="jp-Notebook"' in htmlcont.decode()
        
    finally:
        with open(conf_file, 'w') as fd:
            fd.write(conf_bk)


@pytest.mark.parametrize("run_asynch", [True, False])
def test_return_progress(dispatcher_live_fixture, mock_backend, run_asynch):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {'instrument': 'example0',
              'query_status': 'new',
              'query_type': 'Real',
              'product_type': 'lightcurve',
              'api': 'True',
              'run_asynch': run_asynch,
              'return_progress': True}

    c = requests.get(os.path.join(server, "run_analysis"),
                     params=params)
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert 'progress_product_list' in jdata['products']
    with open(os.path.join(os.path.dirname(__file__), 'responses', 'test_output.html'), 'r') as fd:
        test_output_html = fd.read()

    assert jdata['products']['progress_product_list'][0]['value'] == test_output_html


@pytest.mark.parametrize("api", [True, False])
def test_api_return_progress(dispatcher_live_fixture, mock_backend, api):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {'instrument': 'example0',
              'query_status': 'new',
              'query_type': 'Real',
              'product_type': 'lightcurve',
              'run_asynch': True,
              'return_progress': True}
    if api:
        params['api'] = True

    c = requests.get(os.path.join(server, "run_analysis"),
                     params=params)
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    with open(os.path.join(os.path.dirname(__file__), 'responses', 'test_output.html'), 'r') as fd:
        test_output_html = fd.read()
    if api:
        assert 'progress_product_list' in jdata['products']
        assert jdata['products']['progress_product_list'][0]['value'] == test_output_html
    else:
        assert 'progress_product_html_output' in jdata['products']
        assert jdata['products']['progress_product_html_output'][0] == test_output_html


@pytest.mark.fullstack
def test_structured_param(live_nb2service,
                          conf_file, 
                          dispatcher_live_fixture):
    with open(conf_file, 'r') as fd:
        conf_bk = fd.read()
      
    try:
        with open(conf_file, 'w') as fd:
            fd.write( config_real_nb2service % live_nb2service )
        
        server = dispatcher_live_fixture
        logger.info("constructed server: %s", server)    

        #ensure new conf file readed 
        c = requests.get(server + "/reload-plugin/dispatcher_plugin_nb2workflow")
        assert c.status_code == 200 
        
        from oda_api.api import DispatcherAPI
        disp = DispatcherAPI(url=server)
        prod = disp.get_product(instrument = "example", 
                                product = "structured",
                                struct_par = {'col1': ['spam', 'ham'],
                                              'col2': [5, 1],
                                              'col3': [7.1, 8.2]}
                                )
        
        assert (prod.myname_0.table['col1'] == ['spam', 'ham']).all()
        assert (prod.myname_0.table['col2'] == [5, 1]).all()
        assert (prod.myname_0.table['col3'] == [7.1, 8.2]).all()
        
    finally:
        with open(conf_file, 'w') as fd:
            fd.write(conf_bk)