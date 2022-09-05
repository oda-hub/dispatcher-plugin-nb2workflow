from cdci_data_analysis.pytest_fixtures import (
            dispatcher_live_fixture, 
            dispatcher_debug,
            dispatcher_test_conf,
            dispatcher_test_conf_fn,
            app
        )
import pytest

@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", 9393)

