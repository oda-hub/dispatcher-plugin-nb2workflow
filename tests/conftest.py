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
from xprocess import ProcessStarter
import requests
from urllib.parse import urlparse, parse_qs
from werkzeug.wrappers import Request
from werkzeug.wrappers import Response

from pytest_httpserver.httpserver import MappingQueryMatcher

config_one_instrument = """    
instruments:
  example0:
    data_server_url: http://localhost:8000
    dummy_cache: ""
"""

backend_status_fn = "Backend-status.state"
trace_backend_status_fn = "Trace-Backend-status.state"


def set_backend_status(value):
    open(backend_status_fn, "w").write(value)


def get_backend_status():
    if os.path.exists(backend_status_fn):
        return open(backend_status_fn).read()
    else:
        return ''


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", 8000)


def lightcurve_handler(request: Request):
    parsed_request_query = parse_qs(urlparse(request.url).query)
    async_request = parsed_request_query.get('_async_request', ['no'])
    responses_path = os.path.join(os.path.dirname(__file__), 'responses')

    backend_status = get_backend_status()

    if backend_status == 'fail':
        return Response("backend failure", status=500, content_type=' text/plain')
    elif backend_status == 'trace_fail':
        return Response('{"workflow_status": "done", "data": {}}', status=200, content_type='application/json')
    else:
        if async_request[0] == 'yes':
            with open(os.path.join(responses_path, 'lightcurve_async.json'), 'r') as fd:
                runjson_async = json.loads(fd.read())
            response_data = json.dumps(runjson_async, indent=4)
            return Response(response_data, status=200, content_type='application/json')
        else:
            with open(os.path.join(responses_path, 'lightcurve.json'), 'r') as fd:
                runjson = json.loads(fd.read())
            response_data = json.dumps(runjson, indent=4)
            return Response(response_data, status=200, content_type='application/json')


@pytest.fixture
def mock_backend(httpserver):
    responses_path = os.path.join(os.path.dirname(__file__), 'responses')
    with open(os.path.join(responses_path, 'options.json'), 'r') as fd:
        respjson = json.loads(fd.read())
    with open(os.path.join(responses_path, 'table.json'), 'r') as fd:
        table_json = json.loads(fd.read())
    with open(os.path.join(responses_path, 'ascii_binary.json'), 'r') as fd:
        bin_json = json.loads(fd.read())
    with open(os.path.join(responses_path, 'image.json'), 'r') as fd:
        image_json = json.loads(fd.read())
    with open(os.path.join(responses_path, 'test_output.html'), 'r') as fd:
        test_output_html = fd.read()
        
    httpserver.expect_request('/').respond_with_data('')    
    httpserver.expect_request(f'/api/v1.0/options').respond_with_json(respjson)
    httpserver.expect_request(f'/api/v1.0/get/lightcurve').respond_with_handler(lightcurve_handler)
    httpserver.expect_request(f'/api/v1.0/get/table').respond_with_json(table_json)
    httpserver.expect_request(f'/api/v1.0/get/ascii_binary').respond_with_json(bin_json)
    httpserver.expect_request(f'/api/v1.0/get/image').respond_with_json(image_json)
    httpserver.expect_request(f'/trace/nb2w-ylp5ovnm/lightcurve').respond_with_data(test_output_html)

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
    
@pytest.fixture(scope="session")
def live_nb2service(xprocess):
    wd = os.getcwd()
    class Starter(ProcessStarter):
        pattern = "Serving Flask app"
        timeout = 30
        max_read_lines = 10000 
        terminate_on_interrupt = True
        args = ['nb2service', '--port', '9393', os.path.join(wd, 'tests', 'example_nb')]
        def startup_check(self):
            try: 
                res = requests.get('http://localhost:9393/')
            except requests.ConnectionError:
                return False
            if res.status_code != 200:
                return False
            return res.json()['message'] == 'all is ok!'
    try:
        logfile = xprocess.ensure("nb2service", Starter)
    except Exception as e:
        xprocess.getinfo("nb2service").terminate()
        raise e
    yield 'http://localhost:9393/'
    xprocess.getinfo("nb2service").terminate()
