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
from copy import copy

import logging
logger = logging.getLogger(__name__)


def kg_select(t, kg_conf_dict):
    if kg_conf_dict is None or kg_conf_dict == {}:
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

def get_static_instr_conf(conf_file):
    masked_conf_file = conf_file
    
    # kg_conf_dict = {'type': 'query-service',  
    #                 'path': "https://www.astro.unige.ch/mmoda/dispatch-data/gw/odakb/query"}
    
    cfg_dict = {'instruments': {}, 'kg': {}, 'include_glued_output': True}
    
    if conf_file is not None:
        with open(conf_file, 'r') as ymlfile:
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
                    cfg_dict['kg'] = f_cfg_dict['kg']
                if 'include_glued_output' in f_cfg_dict.keys():
                    cfg_dict['include_glued_output'] = f_cfg_dict['include_glued_output']
            else:
                masked_conf_file = None
    return cfg_dict, masked_conf_file

static_config_dict, masked_conf_file = get_static_instr_conf(conf_file)

if 'ODA_ONTOLOGY_PATH' in os.environ:
    ontology_path = os.environ.get('ODA_ONTOLOGY_PATH')
else:
    ontology_path = static_config_dict.get('ontology_path', 
                                    'http://odahub.io/ontology/ontology.ttl')
logger.info('Using ontology from %s', ontology_path)

def get_config_dict_from_kg(kg_conf_dict=static_config_dict['kg']):
    cfg_dict = {'instruments': {}}
    
    for r in kg_select('''
                ?w a <http://odahub.io/ontology#WorkflowService>;
                <http://odahub.io/ontology#deployment_name> ?deployment_name;
                <http://odahub.io/ontology#service_name> ?service_name .
                OPTIONAL {
                    ?w <https://schema.org/creativeWorkStatus> ?work_status .
                }
            ''', kg_conf_dict):

        logger.info('found instrument service record %s', r)
        cfg_dict['instruments'][r['service_name']['value']] = {
            "data_server_url": f"http://{r['deployment_name']['value']}:8000",
            "dummy_cache": "",
            "creativeWorkStatus": r.get('work_status', {'value': 'undefined'})['value'], 
                # creativeWorkStatus isn't currently used further in plugin but may be used in the future. Useful in test, though.
            "restricted_access": False if r.get('work_status', {'value': 'undefined'})['value'] == "production" else True
        }
    
    return cfg_dict

combined_instrument_dict = {}
def build_combined_instrument_dict():
    global combined_instrument_dict
    combined_instrument_dict = copy(static_config_dict.get('instruments', {}))
    combined_instrument_dict.update(get_config_dict_from_kg()['instruments'])

build_combined_instrument_dict()

def factory_factory(instr_name, restricted_access):
    instrument_query = NB2WInstrumentQuery('instr_query', restricted_access)
    def instr_factory():
        backend_options = NB2WDataDispatcher(instrument=instr_name).backend_options
        query_list, query_dict = NB2WProductQuery.query_list_and_dict_factory(backend_options, 
                                                                              ontology_path)
        return Instrument(instr_name,
                        src_query = NB2WSourceQuery.from_backend_options(backend_options, 
                                                                         ontology_path),
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
        build_combined_instrument_dict()
        
        current_instrs = [x.instr_name for x in self.lst]
        available_instrs = combined_instrument_dict.keys()
        new_instrs = set(available_instrs) - set(current_instrs)
        old_instrs = set(current_instrs) - set(available_instrs)
        keep_instrs = set(available_instrs) & set(current_instrs)
        
        if old_instrs:
            for instr in old_instrs:
                idx = current_instrs.index(instr)
                self.lst.pop(idx)
        
        if new_instrs:
            for instr in new_instrs:
                self.lst.append(factory_factory(instr, combined_instrument_dict[instr].get('restricted_access', False)))
        
        # check if some instruments changed status
        if keep_instrs:
            for instr in keep_instrs:
                idx = current_instrs.index(instr)
                # only nb2w instruments may be affected. We don't want to instantiate any instrument here
                instr_query = getattr(self.lst[idx], 'instrument_query', None)
                if ( instr_query is not None and
                     instr_query.restricted_access != combined_instrument_dict[instr].get('restricted_access', False) ):
                    self.lst[idx] = factory_factory(instr, combined_instrument_dict[instr].get('restricted_access', False))

    def __iter__(self):
        self._update_instruments_list()
        return self.lst.__iter__()    
                    
instr_factory_list = [ factory_factory(instr_name, instr_conf.get('restricted_access', False)) 
                       for instr_name, instr_conf in combined_instrument_dict.items()]

instr_factory_list = NB2WInstrumentFactoryIter(instr_factory_list)
