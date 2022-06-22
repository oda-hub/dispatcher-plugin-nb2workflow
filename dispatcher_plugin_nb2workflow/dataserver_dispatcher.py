from cdci_data_analysis.analysis.queries import QueryOutput
from cdci_data_analysis.configurer import DataServerConf
import requests
import time 

class NB2WDataDispatcher:
    def __init__(self, instrument=None, param_dict=None, task=None, config=None):
        if config is None:
            config = DataServerConf.from_conf_dict(instrument.data_server_conf_dict)
        self.data_server_url = config.data_server_url
        self.task = task
        self.param_dict = param_dict
        
    @staticmethod    
    def query_backend_options(data_server_url):
        url = data_server_url.strip('/') + '/api/v1.0/options'
        try:
            res = requests.get("%s" % (url), params=None)
        except:
            return {}
        if res.status_code == 200:
            options_dict = res.json()
        else:
            return {}
            raise ConnectionError(f"Backend connection failed: {res.status_code}")
            # TODO: consecutive requests if failed
        return options_dict
        
        
    def test_communication(self, max_trial=10, sleep_s=1, logger=None):
        print('--> start test connection')

        query_out = QueryOutput()
        no_connection = True
        excep = Exception()
        
        print('url', self.data_server_url)
        url = self.data_server_url

        for i in range(max_trial):
            try:
                res = requests.get("%s" % (url), params=None)
                print('status_code',res.status_code)
                if res.status_code !=200:
                    no_connection =True
                    raise ConnectionError(f"Backend connection failed: {res.status_code}")
                else:
                    no_connection=False

                    message = 'Connection OK'
                    query_out.set_done(message=message, debug_message='OK')
                    print('-> test connections passed')
                    break
            except Exception as e:
                excep = e
                no_connection = True

            time.sleep(sleep_s)

        if no_connection is True:
            query_out.set_query_exception(excep, 
                                          'no data server connection',
                                          logger=logger)
            raise ConnectionError('Backend connection failed')

        return query_out
    

    def test_has_input_products(self, instrument, logger=None):
        query_out = QueryOutput()
        query_out.set_done('input products check skipped')
        return query_out, []

    def run_query(self,
                  call_back_url = None,
                  run_asynch = True,
                  logger = None,
                  task = None,
                  param_dict = None):
        
        res = None
        message = ''
        debug_message = ''
        query_out = QueryOutput()

        if task is None:
            task=self.task     

        if param_dict is None:
            param_dict=self.param_dict   
        
        if run_asynch:
            param_dict['_async_request_callback'] = call_back_url
            param_dict['_async_request'] = "yes"

        url = '/'.join([self.data_server_url.strip('/'), 'api/v1.0/get', task.strip('/')])
        res = requests.get(url, params = param_dict)
        if res.status_code == 200:
            if 'data' in res.json().keys() and res.json()['data']['exceptions']: #failed nb execution in async 
                except_message = res.json()['data']['exceptions'][0]['ename']+': '+res.json()['data']['exceptions'][0]['evalue']
                query_out.set_failed('Processing failed', 
                                     message=except_message)
                raise RuntimeError(f'Processing failed. {except_message}')
            query_out.set_done(message=message, debug_message=str(debug_message),job_status='done')
        elif res.status_code == 201:
            if res.json()['workflow_status'] == 'submitted':
                query_out.set_status(0, message=message, debug_message=str(debug_message),job_status='submitted')
            else:
                query_out.set_status(0, message=message, debug_message=str(debug_message),job_status='progress')
                #this anyway finally sets "submitted", the only status implemented now in "non-integral" dispatcher code
        else:
            try:
                query_out.set_failed('Error in the backend', 
                                 message='connection status code: ' + str(res.status_code), 
                                 extra_message=res.json()['exceptions'][0])
            except:
                query_out.set_failed('Error in the backend', 
                                 message='connection status code: ' + str(res.status_code), 
                                 extra_message = res.text)
            raise RuntimeError('Error in the backend')

        return res, query_out
