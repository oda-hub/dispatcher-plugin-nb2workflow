from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import SourceQuery, InstrumentQuery
from .queries import NB2WProductQuery, NB2WInstrumentQuery, NB2WSourceQuery
from .dataserver_dispatcher import NB2WDataDispatcher
from . import conf_file
import json
import yaml
import requests
import rdflib as rdf
import os

import logging
logger = logging.getLogger(__name__)


def kg_select(t, kg_conf_dict):
    if kg_conf_dict is None:
        logger.info('Not using KG to get instruments')
        qres_js = []
    elif kg_conf_dict.get('type') == 'query-service':
        r = requests.get(kg_conf_dict['path'],
                    params={"query": f"""
                        SELECT * WHERE {{
                            {t}
                        }} LIMIT 100
                    """})
                    
        if r.status_code != 200:
            raise RuntimeError(f'{r}: {r.text}')
        
        qres_js = r.json()['results']['bindings']

    elif kg_conf_dict.get('type') == 'file':
        graph = rdf.Graph()
        if os.path.isfile(kg_conf_dict['path']):
            graph.parse(kg_conf_dict['path'])
        else:
            logger.warning("Knowledge graph file %s doesn't exist yet. " 
                           "No instruments information will be loaded.", 
                           kg_conf_dict['path'])
        qres = graph.query(f"""
                        SELECT * WHERE {{
                            {t}
                        }} LIMIT 100
                    """)
        qres_js = json.loads(qres.serialize(format='json'))['results']['bindings']
    
    else:
        logger.warning('Unknown KG type')
        qres_js = []
            
    logger.warning(json.dumps(qres_js, indent=4))
    
    return qres_js

def get_instrs_from_kg(kg_conf_dict):
    instruments = {}
    for r in kg_select('''
        ?w a <http://odahub.io/ontology#WorkflowService>;
        <http://odahub.io/ontology#deployment_name> ?deployment_name;
        <http://odahub.io/ontology#service_name> ?service_name ;
        <https://schema.org/creativeWorkStatus>?  ?work_status .               
    ''', kg_conf_dict): 

        logger.info('found instrument service record %s', r)
        instruments[r['service_name']['value']] = {
            "data_server_url": f"http://{r['deployment_name']['value']}:8000",
            "dummy_cache": "",
            "restricted_access": False if r['work_status']['value'] == "production" else True
        }
    
    return instruments
    
    
def get_instr_conf(from_conf_file=None):
    masked_conf_file = from_conf_file
    
    # current default - query central oda kb 
    # TODO: better default will be some regullary updated static location
    kg_conf_dict = {'type': 'query-service',  
                    'path': "https://www.astro.unige.ch/mmoda/dispatch-data/gw/odakb/query"}
    cfg_dict = {'instruments': {}}
    
    if from_conf_file is not None:
        with open(from_conf_file, 'r') as ymlfile:
            f_cfg_dict = yaml.load(ymlfile, Loader=yaml.SafeLoader)
            if f_cfg_dict is not None:
                if 'instruments' in f_cfg_dict.keys():
                    cfg_dict['instruments'] = f_cfg_dict['instruments']
                else:
                    masked_conf_file = None 
                    # need to set to None as it's being read inside Instrument
                if 'ontology_path' in f_cfg_dict.keys():
                    cfg_dict['ontology_path'] = f_cfg_dict['ontology_path']
                if 'kg' in f_cfg_dict.keys():
                    kg_conf_dict = f_cfg_dict['kg']
            else:
                masked_conf_file = None
    
    cfg_dict['kg_instruments'] = get_instrs_from_kg(kg_conf_dict)
    
    return cfg_dict, masked_conf_file

config_dict, masked_conf_file = get_instr_conf(conf_file)
if 'ODA_ONTOLOGY_PATH' in os.environ:
    ontology_path = os.environ.get('ODA_ONTOLOGY_PATH')
else:
    ontology_path = config_dict.get('ontology_path', 
                                    'http://odahub.io/ontology/ontology.ttl')
logger.info('Using ontology from %s', ontology_path)

def factory_factory(instr_name, restricted_access):
    instrument_query = NB2WInstrumentQuery('instr_query', restricted_access)
    def instr_factory():
        backend_options = NB2WDataDispatcher(instrument=instr_name).backend_options
        query_list, query_dict = NB2WProductQuery.query_list_and_dict_factory(backend_options, ontology_path)
        return Instrument(instr_name,
                        src_query = NB2WSourceQuery.from_backend_options(backend_options, ontology_path),
                        instrumet_query = instrument_query,
                        data_serve_conf_file=masked_conf_file,
                        product_queries_list=query_list,
                        query_dictionary=query_dict,
                        asynch=True, 
                        data_server_query_class=NB2WDataDispatcher,
                        )
    instr_factory.instr_name = instr_name
    instr_factory.instrument_query = instrument_query
    return instr_factory

class NB2WInstrumentFactoryIter:
    def __init__(self, lst):
        self.lst = lst
    
    def _update_instruments_list(self):
        static_instrs = config_dict['instruments']
        kg_instrs = get_instrs_from_kg(config_dict)
        
        current_instrs = [x.instr_name for x in self.lst]
        available_instrs = static_instrs.keys() + kg_instrs.keys()
        new_instrs = set(available_instrs) - set(current_instrs)
        old_instrs = set(current_instrs) - set(available_instrs)
        
        if old_instrs:
            for instr in old_instrs:
                idx = current_instrs.index(instr)
                self.lst.pop(idx)
        
        if new_instrs:
            for instr in new_instrs:
                self.lst.append(factory_factory(instr, kg_instrs[instr].get('restricted_access', False)))
        
    def __iter__(self):
        self._update_instruments_list()
        return self.lst.__iter__()    
                    
instr_factory_list = [ factory_factory(instr_name, instr_conf.get('restricted_access', False)) 
                       for instr_name, instr_conf in 
                       config_dict['instruments'].items() + config_dict['kg_instruments'].items() ]
