from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import SourceQuery, InstrumentQuery
from .queries import NB2WProductQuery
from .dataserver_dispatcher import NB2WDataDispatcher
from . import conf_file
import yaml

def read_conf_file(conf_file):
    cfg_dict = None
    if conf_file is not None:
        with open(conf_file, 'r') as ymlfile:
            cfg_dict = yaml.load(ymlfile, Loader=yaml.SafeLoader)
    return cfg_dict

config_dict = read_conf_file(conf_file)

def factory_factory(instr_name, data_server_url):
    def instr_factory():
        query_list, query_dict = NB2WProductQuery.query_list_and_dict_factory(data_server_url)
        return Instrument(instr_name,
                        src_query = SourceQuery('src_query'),
                        instrumet_query = InstrumentQuery('instr_query'),
                        data_serve_conf_file=conf_file,
                        product_queries_list=query_list,
                        query_dictionary=query_dict,
                        asynch=True, 
                        data_server_query_class=NB2WDataDispatcher,
                        )
    return instr_factory

instr_factory_list = [ factory_factory(instr_name, instr_conf['data_server_url']) 
                       for instr_name, instr_conf in config_dict['instruments'].items() ]
