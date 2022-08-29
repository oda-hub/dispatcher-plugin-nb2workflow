from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import SourceQuery, InstrumentQuery
from .queries import NB2WProductQuery, NB2WInstrumentQuery
from .dataserver_dispatcher import NB2WDataDispatcher
from . import conf_file
import json
import yaml
import requests

import logging
logger = logging.getLogger(__name__)


def kg_select(t):
    #TODO: use fragment or static regularly updated location instead, for performance and resilience
    r = requests.get("https://www.astro.unige.ch/mmoda/dispatch-data/gw/odakb/query",
                params={"query": f"""
                    SELECT * WHERE {{
                        {t}
                    }} LIMIT 100
                """})
                
    if r.status_code != 200:
        raise RuntimeError(f'{r}: {r.text}')

    logger.warning(json.dumps(r.json()['results']['bindings'], indent=4))

    return r.json()['results']['bindings']



def read_conf_file(conf_file):
    if conf_file is None:
        return {}
        raise RuntimeError('unable to read config file!')
    else:
        with open(conf_file, 'r') as ymlfile:
            cfg_dict = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    
    for r in kg_select('''
            ?w a <http://odahub.io/ontology#WorkflowService>;
               <http://odahub.io/ontology#deployment_name> ?deployment_name;
               <http://odahub.io/ontology#service_name> ?service_name .               
        '''): 

        logger.info('found instrument service record %s', r)
        cfg_dict['instruments'][r['service_name']['value']] = {
            "data_server_url": f"http://{r['deployment_name']['value']}:8000",
            "dummy_cache": ""
        }
    
    return cfg_dict

config_dict = read_conf_file(conf_file)

def factory_factory(instr_name, data_server_url):
    def instr_factory():
        query_list, query_dict = NB2WProductQuery.query_list_and_dict_factory(data_server_url)
        return Instrument(instr_name,
                        src_query = SourceQuery('src_query'),
                        instrumet_query = NB2WInstrumentQuery('instr_query'),
                        data_serve_conf_file=conf_file,
                        product_queries_list=query_list,
                        query_dictionary=query_dict,
                        asynch=True, 
                        data_server_query_class=NB2WDataDispatcher,
                        )
    return instr_factory

instr_factory_list = [ factory_factory(instr_name, instr_conf['data_server_url']) 
                       for instr_name, instr_conf in config_dict['instruments'].items() ]
