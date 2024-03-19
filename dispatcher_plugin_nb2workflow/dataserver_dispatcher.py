from cdci_data_analysis.analysis.queries import QueryOutput
from cdci_data_analysis.configurer import DataServerConf
from cdci_data_analysis.analysis.parameters import subclasses_recursive
import requests
import time 
from . import exposer
from urllib.parse import urlsplit, parse_qs, urlencode
import os
from glob import glob
import logging

logger = logging.getLogger()

class NB2WDataDispatcher:
    def __init__(self, instrument=None, param_dict=None, task=None, config=None):
        iname = instrument if isinstance(instrument, str) else instrument.name
        if config is None:
            config = DataServerConf.from_conf_dict(exposer.combined_instrument_dict[iname])

        self.include_glued_output = exposer.static_config_dict.get('include_glued_output', True)
        self.data_server_url = config.data_server_url
        self.task = task
        self.param_dict = param_dict
        
        self.external_disp_url = None 
        if not isinstance(instrument, str): # TODO: seems this is always the case. But what if not?
            products_url_config = instrument.disp_conf.products_url
            parsed = urlsplit(products_url_config)
            if parsed.scheme and parsed.netloc:
                self.external_disp_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        
    @property
    def backend_options(self, max_trial=5, sleep_seconds=5):
        try:
            options_dict = self._backend_options
        except AttributeError:
            url = self.data_server_url.strip('/') + '/api/v1.0/options'
            for i in range(max_trial):
                try:
                    res = requests.get("%s" % (url), params=None)

                    if res.status_code == 200:
                        options_dict = res.json()
                        backend_available = True
                        break
                    else:
                        raise RuntimeError("Backend options request failed. " 
                                           f"Exit code: {res.status_code}. "
                                           f"Response: {res.text}")
                except Exception as e:
                    backend_available = False
                    logger.error(f"Exception while getting backend options {repr(e)}")
                    time.sleep(sleep_seconds)
            if not backend_available:
                return {}

            self._backend_options = options_dict
        return options_dict
        
    def get_backend_comment(self, product):
        comment_uri = 'http://odahub.io/ontology#WorkflowResultComment'
        if self.backend_options.get(product):
            for field, desc in self.backend_options[product].get('output', {}).items():
                if desc.get('owl_type') == comment_uri:
                    return field
        return None
            
        
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

    def get_progress_run(self,
                         call_back_url=None,
                         run_asynch=None,
                         logger=None,
                         task=None,
                         param_dict=None):

        query_out = QueryOutput()
        res_trace_dict = None

        if task is None:
            task = self.task
        if param_dict is None:
            param_dict = self.param_dict
        if run_asynch and call_back_url is not None:
            param_dict['_async_request_callback'] = call_back_url
            param_dict['_async_request'] = "yes"

        url = os.path.join(self.data_server_url, 'api/v1.0/get', task.strip('/'))
        res = requests.get(url, params=param_dict)
        if res.status_code in [200, 201]:
            res_data = res.json()
            workflow_status = res_data['workflow_status'] if run_asynch else 'done'
            if workflow_status == 'started' or workflow_status == 'done':
                resroot = res_data['data'] if run_asynch and workflow_status == 'done' else res_data
                jobdir = resroot.get('jobdir', None)
                if jobdir is not None:
                    jobdir = jobdir.split('/')[-1]
                    trace_url = os.path.join(self.data_server_url, 'trace', jobdir, task.strip('/'))
                    query_string = {}
                    if not self.include_glued_output:
                        query_string = {'include_glued_output': False}
                    res_trace = requests.get(trace_url, params=query_string)
                    res_trace_dict = {
                        'res': res_trace,
                        'progress_product': True
                    }
            workflow_status = 'progress' if workflow_status == 'started' else workflow_status
            query_out.set_status(0, job_status=workflow_status)
        else:
            if 'application/json' in res.headers.get('content-type', ''):
                e_message = res.json()['exceptions'][0]
            else:
                e_message = res.text
            query_out.set_failed('Error in the backend',
                                 message='connection status code: ' + str(res.status_code),
                                 e_message=e_message)
            logger.error(f'Error in the backend, connection status code: {str(res.status_code)}. '
                         f'error: \n{e_message}')

        return res_trace_dict, query_out

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

        info_obj = self.extract_info_from_callback_url(call_back_url)
        if self.external_disp_url is not None:
            basepath = os.path.join(self.external_disp_url, 'dispatch-data/download_file')
        else:
            basepath = f"{info_obj['scheme']}://{info_obj['netloc']}{info_obj['path'].replace('call_back', 'download_file')}"

        param_dict = self.update_param_dict_download_file_url(param_dict,
                                                              task,
                                                              info_obj['session_id'],
                                                              info_obj['job_id'],
                                                              info_obj['instrument_name'],
                                                              info_obj['token'],
                                                              basepath)

        url = '/'.join([self.data_server_url.strip('/'), 'api/v1.0/get', task.strip('/')])
        res = requests.get(url, params = param_dict)
        if res.status_code == 200:
            resroot = res.json()['data'] if run_asynch else res.json()
            
            except_message = None
            if resroot['exceptions']: 
                if isinstance(resroot['exceptions'][0], dict): # in async
                    except_message = resroot['exceptions'][0]['ename']+': '+res.json()['data']['exceptions'][0]['evalue']
                else:
                    except_message = res.json()['exceptions'][0]
                                                            
                query_out.set_failed('Backend exception', 
                                    message='Backend failed. ' + except_message,
                                    job_status='failed')
                return res, query_out

            comment_name = self.get_backend_comment(task.strip('/'))
            comment_value = ''
            if comment_name:
                if run_asynch: 
                    comment_value = res.json()['data']['output'][comment_name]
                else:
                    comment_value = res.json()['output'][comment_name]
        
            query_out.set_done(message=message, debug_message=str(debug_message),job_status='done', comment=comment_value)
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

        return res, query_out

    # def update_param_dict_download_file_url(self, param_dict, task, session_id, job_id, instrument_name, token, basepath, ontology_object=None):
    #     parameter_hierarchy = ontology_object.get_parameter_hierarchy("http://odahub.io/ontology#POSIXPath")
    #     for param in param_dict:
    #         param_obj = self.backend_options[task]['parameters'].get(param, None)
    #         # TODO improve this check, is it enough?
    #         if param_obj is not None and param_dict[param] != '':
    #                 # and param_obj.get('owl_type') == "http://odahub.io/ontology#POSIXPath" \
    #             for owl_superclass_uri in parameter_hierarchy:
    #                 for python_subclass in subclasses_recursive(param_obj.__class__):
    #                     if python_subclass.matches_owl_uri(owl_superclass_uri):
    #                         dpars = urlencode(dict(session_id=session_id,
    #                                                job_id=job_id,
    #                                                file_list=param,
    #                                                query_status="ready",
    #                                                instrument=instrument_name,
    #                                                token=token))
    #                         download_file_url = f"{basepath}?{dpars}"
    #                         param_dict[param] = download_file_url
    #
    #     return param_dict

    def update_param_dict_download_file_url(self, param_dict, task, session_id, job_id, instrument_name, token, basepath):
        for param in param_dict:
            # param_obj = self.backend_options[task]['parameters'].get(param, None)
            # if param_obj is not None \
            #         and param_obj.get('owl_type') == "http://odahub.io/ontology#POSIXPath" \
            #         and param_dict[param] != '':
            # TODO improve this check, is it enough?
            if (param_dict[param] != ''
                    and isinstance(param_dict[param], dict)
                    and param_dict[param].get('downloadable', False)):
                dpars = urlencode(dict(session_id=session_id,
                                       job_id=job_id,
                                       file_list=param,
                                       query_status="ready",
                                       instrument=instrument_name,
                                       token=token))
                download_file_url = f"{basepath}?{dpars}"
                param_dict[param] = download_file_url
        return param_dict

    def extract_info_from_callback_url(self, url):
        spl_cb_url = urlsplit(url)
        qpars = parse_qs(spl_cb_url[3])
        info_obj = dict(
            scheme=spl_cb_url[0],
            netloc=spl_cb_url[1],
            path=spl_cb_url[2],
            session_id=qpars['session_id'][0],
            job_id=qpars['job_id'][0],
            token=qpars['token'][0],
            instrument_name=qpars['instrument_name'][0]
        )

        return info_obj
