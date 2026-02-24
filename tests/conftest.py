from cdci_data_analysis.pytest_fixtures import (
            kill_child_processes,
            dispatcher_debug,
            dispatcher_test_conf_fn,
            dispatcher_test_conf_with_external_products_url_fn,
            dispatcher_test_conf_with_default_route_products_url_fn,
            dispatcher_test_conf,
            dispatcher_test_conf_with_external_products_url,
            dispatcher_test_conf_with_default_route_products_url,
            dispatcher_live_fixture,
            dispatcher_live_fixture_with_external_products_url,
            dispatcher_live_fixture_with_default_route_products_url,
            gunicorn_dispatcher,
            gunicorn_dispatcher_live_fixture,
        )
import pytest
import json
import os
import requests
import subprocess
import sys
import socket
import shutil
import hashlib

from pathlib import Path
from xprocess import ProcessStarter
from urllib.parse import urlparse, parse_qs
from werkzeug.wrappers import Request, Response


config_one_instrument = """   
include_glued_output: True
instruments:
  example0:
    data_server_url: http://localhost:8000
    dummy_cache: ""
"""

config_one_instrument_no_glued_output = """
include_glued_output: False
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
        return Response('{"workflow_status": "failed", "data": {}}', status=500, content_type='application/json')
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


def trace_get_func_handler(request: Request):
    parsed_request_query = parse_qs(urlparse(request.url).query)
    include_glued_output = parsed_request_query.get('include_glued_output', ['True']) == ['True']
    responses_path = os.path.join(os.path.dirname(__file__), 'responses')

    output_html_file = 'test_output.html'

    if not include_glued_output:
        output_html_file = 'test_output_no_glue_output.html'

    with open(os.path.join(responses_path, output_html_file), 'r') as fd:
        test_output_content = fd.read()

    return Response(test_output_content, status=200)

def return_request_query_dict(request: Request):
    parsed_request_query = parse_qs(urlparse(request.url).query)
    resp = '{"exceptions": [], "output": {"result": '+json.dumps(parsed_request_query)+'}}'
    return Response(resp,
                    status=200, content_type='application/json')
    

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
    with open(os.path.join(responses_path, 'data_product.json'), 'r') as fd:
        data_product_json = json.loads(fd.read())
    # with open(os.path.join(responses_path, 'test_output.html'), 'r') as fd:
    #     test_output_html = fd.read()
        
    httpserver.expect_request('/').respond_with_data('')    
    httpserver.expect_request(f'/api/v1.0/options').respond_with_json(respjson)
    httpserver.expect_request(f'/api/v1.0/get/lightcurve').respond_with_handler(lightcurve_handler)
    httpserver.expect_request(f'/api/v1.0/get/table').respond_with_json(table_json)
    httpserver.expect_request(f'/api/v1.0/get/ascii_binary').respond_with_json(bin_json)
    httpserver.expect_request(f'/api/v1.0/get/image').respond_with_json(image_json)
    httpserver.expect_request(f'/api/v1.0/get/data_product').respond_with_json(data_product_json)
    httpserver.expect_request(f'/api/v1.0/get/data_product_no_annotations').respond_with_json(data_product_json)
    # httpserver.expect_request(f'/trace/nb2w-ylp5ovnm/lightcurve').respond_with_data(test_output_html)
    httpserver.expect_request(f'/trace/nb2w-ylp5ovnm/lightcurve').respond_with_handler(trace_get_func_handler)
    httpserver.expect_request(f'/api/v1.0/get/dummy_echo').respond_with_handler(return_request_query_dict)

@pytest.fixture(scope='session')
def conf_file(tmp_path_factory):
    d = tmp_path_factory.mktemp('nb2wconf')
    fn = d / 'plugin_conf.yml'
    fn.write_text(config_one_instrument)
    yield str(fn.resolve())


@pytest.fixture(scope='session')
def conf_file_no_glued_output(tmp_path_factory):
    d = tmp_path_factory.mktemp('nb2wconf')
    fn = d / 'plugin_conf.yml'
    fn.write_text(config_one_instrument_no_glued_output)
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


@pytest.fixture
def set_env_var_plugin_config_no_glued_output_file_path(conf_file_no_glued_output):
    old_environ = dict(os.environ)
    os.environ['CDCI_NB2W_PLUGIN_CONF_FILE'] = conf_file_no_glued_output
    yield
    os.environ.clear()
    os.environ.update(old_environ)


@pytest.fixture(scope="session")
def live_nb2service(xprocess):
    venv_base_dir = Path(__file__).parent / ".webserver_venv_cache"
    pyproject_dir = Path(__file__).parent / "webserver"
    pyproject = pyproject_dir / "pyproject.toml"
    lockfile = pyproject_dir / "uv.lock"
    logfile = Path(__file__).parent / ".webserver.log"

    def hash_inputs() -> str:
        h = hashlib.sha256()
        h.update(pyproject.read_bytes())
        if lockfile.exists():
            h.update(lockfile.read_bytes())
        return h.hexdigest()[:16]

    venv_path = venv_base_dir / hash_inputs()

    def find_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    def python_path() -> Path:
        return venv_path / "bin" / "python"

    def pip_path() -> Path:
        return venv_path / "bin" / "pip"

    def create_venv():
        if venv_path.exists():
            return  # cache hit

        venv_path.parent.mkdir(exist_ok=True)

        if shutil.which("uv"):
            subprocess.run(["uv", "venv", str(venv_path)], check=True)
            subprocess.run(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(python_path()),
                    str(pyproject_dir),
                ],
                check=True,
            )
        else:
            subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)
            subprocess.run(
                [str(pip_path()), "install", "-e", str(pyproject_dir)],
                check=True,
            )

    port = find_free_port()
    url = f"http://127.0.0.1:{port}"

    create_venv()

    class Starter(ProcessStarter):
        pattern = ".*"
        timeout = 20
        terminate_on_interrupt = True
        responses_path = Path(__file__).parent / 'example_nb'
        args = [
            str(venv_path / "bin" / "nb2service"), 
            "--port", 
            str(port), 
            str(responses_path)
            ]

        def startup_check(self):
            try:
                r = requests.get(f"{url}/health", timeout=0.2)
                return r.status_code == 200
            except requests.RequestException:
                return False

    logfile.parent.mkdir(parents=True, exist_ok=True)

    xprocess.ensure(
        "nb2service",
        Starter,
    )

    yield url

    xprocess.getinfo("nb2service").terminate()
    logfile.unlink(missing_ok=True)
    # NOTE: cached venv is intentionally NOT deleted
