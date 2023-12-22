from cdci_data_analysis.analysis.queries import ProductQuery, QueryOutput, BaseQuery, InstrumentQuery
from cdci_data_analysis.analysis.parameters import Parameter, Name
from .products import (NB2WProduct, 
                       NB2WAstropyTableProduct, 
                       NB2WBinaryProduct, 
                       NB2WPictureProduct, 
                       NB2WTextProduct, 
                       NB2WParameterProduct,
                       NB2WProgressProduct)
from .dataserver_dispatcher import NB2WDataDispatcher
from cdci_data_analysis.analysis.ontology import Ontology
import os
from functools import lru_cache
from json import dumps

class HashableDict(dict):
    def __hash__(self):
        return hash(dumps(self))

def with_hashable_dict(func):
    def wrapper(backend_param_dict, ontology_path):
        return func(HashableDict(backend_param_dict), ontology_path)
    return wrapper


@with_hashable_dict
@lru_cache
def construct_parameter_lists(backend_param_dict, ontology_path):
    src_query_pars_uris = { "http://odahub.io/ontology#PointOfInterestRA": "RA",
                            "http://odahub.io/ontology#PointOfInterestDEC": "DEC",
                            "http://odahub.io/ontology#StartTime": "T1",
                            "http://odahub.io/ontology#EndTime": "T2",
                            "http://odahub.io/ontology#AstrophysicalObject": "src_name"}
    par_name_substitution = {}
    
    plist = []
    source_plist = []
    for pname, pval in backend_param_dict.items():
        onto = Ontology(ontology_path)
        if pval.get("extra_ttl"):
            onto.parse_extra_triples(pval.get("extra_ttl"))
        onto_class_hierarchy = onto.get_parameter_hierarchy(pval['owl_type'])
        src_query_owl_uri_set = set(onto_class_hierarchy).intersection(src_query_pars_uris.keys())
        if src_query_owl_uri_set:
            default_pname = src_query_pars_uris[src_query_owl_uri_set.pop()]
            par_name_substitution[ default_pname ] = pname
            source_plist.append(Parameter.from_owl_uri(pval['owl_type'], 
                                                       value=pval['default_value'], 
                                                       name=default_pname,
                                                       ontology_path=ontology_path,
                                                       extra_ttl=pval.get("extra_ttl")
                                                       ))
        else:
            #if param name coincides with the SourceQuery default names, but it's not properly annotated, rename
            cur_name = pname
            if pname in src_query_pars_uris.values():
                cur_name = pname + '_rename'
                par_name_substitution[ cur_name ] = pname
            plist.append(Parameter.from_owl_uri(pval['owl_type'], 
                                                value=pval['default_value'], 
                                                name=cur_name,
                                                ontology_path=ontology_path,
                                                extra_ttl=pval.get("extra_ttl")
                                                ))
    
    return {'source_plist': source_plist,
            'prod_plist': plist,
            'par_name_substitution': par_name_substitution}

class NB2WSourceQuery(BaseQuery):
    @classmethod
    def from_backend_options(cls, backend_options, ontology_path):
        product_names = backend_options.keys()
        # Note that different backend products could contain different sets of the source query parameters. 
        # So we squash them into one list without duplicates
        parameters_dict = {}
        for product_name in product_names:
            backend_param_dict = backend_options[product_name]['parameters']
            prod_source_plist = construct_parameter_lists(backend_param_dict, ontology_path)['source_plist']
            for par in prod_source_plist:
                parameters_dict[par.name] = par
        parameters_list = list(parameters_dict.values())
        parameters_list.append(Name(name_format='str', name='token', value=None))
        return cls('src_query', parameters_list)

class NB2WProductQuery(ProductQuery): 
    def __init__(self, name, backend_product_name, backend_param_dict, backend_output_dict, ontology_path):
        self.backend_product_name = backend_product_name
        self.backend_output_dict = backend_output_dict
        parameter_lists = construct_parameter_lists(backend_param_dict, ontology_path)
        self.par_name_substitution = parameter_lists['par_name_substitution']
        plist = parameter_lists['prod_plist']
        self.ontology_path = ontology_path
        super().__init__(name, parameters_list = plist)
    
    @classmethod
    def query_list_and_dict_factory(cls, backend_options, ontology_path):
        product_names = backend_options.keys()
        qlist = []
        qdict = {}
        for product_name in product_names:
            backend_param_dict = backend_options[product_name]['parameters']
            backend_output_dict = backend_options[product_name]['output']
            qlist.append(cls(f'{product_name}_query', product_name, backend_param_dict, backend_output_dict, ontology_path))
            qdict[product_name] = f'{product_name}_query'
        return qlist, qdict
        
        
    def get_data_server_query(self, instrument, config=None, **kwargs):
        param_dict = {}
        for param_name in instrument.get_parameters_name_list(prod_name = self.backend_product_name):
            param_instance = instrument.get_par_by_name(param_name, prod_name = self.backend_product_name)
            bk_pname = self.par_name_substitution.get(param_name, param_name)
            if getattr(param_instance, 'par_default_format'):
                param_dict[bk_pname] = param_instance.get_value_in_default_format()
            elif getattr(param_instance, 'default_units'):
                param_dict[bk_pname] = param_instance.get_value_in_default_units()
            else:
                param_dict[bk_pname] = param_instance.value
                
        return instrument.data_server_query_class(instrument=instrument,
                                                config=config,
                                                param_dict=param_dict,
                                                task=self.backend_product_name) 
    
    def build_product_list(self, instrument, res, out_dir, api=False, return_progress=False):
        prod_list = []
        _output = None
        if out_dir is None:
            out_dir = './'
        if not return_progress:
            if 'output' in res.json().keys(): # in synchronous mode
                _o_dict = res.json()
            else:
                _o_dict = res.json()['data']
            _output = _o_dict['output']
            prod_list = NB2WProduct.prod_list_factory(self.backend_output_dict, _output, out_dir, self.ontology_path)
        else:
            _o_text = res.content.decode()
            prod_list.append(NB2WProgressProduct(_o_text, out_dir))

        return prod_list
    
    def process_product_method(self, instrument, prod_list, api=False):
        query_out = QueryOutput()
        
        
        np_dp_list, bin_dp_list, tab_dp_list, bin_im_dp_list, text_dp_list, progress_dp_list = [], [], [], [], [], []
        if api is True:
            for product in prod_list.prod_list:
                if isinstance(product, NB2WAstropyTableProduct):
                    tab_dp_list.append(product.dispatcher_data_prod.table_data)
                elif isinstance(product, NB2WBinaryProduct):
                    bin_dp_list.append(product.data_prod)
                elif isinstance(product, NB2WPictureProduct):
                    bin_im_dp_list.append(product.data_prod) 
                elif isinstance(product, NB2WTextProduct):
                    text_dp_list.append({'name': product.name, 'value': product.data_prod})
                elif isinstance(product, NB2WParameterProduct):
                    text_dp_list.append({'name': product.name, 
                                         'value': product.parameter_obj.value, 
                                         'meta_data': {'uri': product.type_key}})
                elif isinstance(product, NB2WProgressProduct):
                    progress_dp_list.append({'name': product.name,
                                             'value': product.progress_data})
                else: # NB2WProduct contains NumpyDataProd by default
                    np_dp_list.append(product.dispatcher_data_prod.data)
                    
            query_out.prod_dictionary['numpy_data_product_list'] = np_dp_list
            query_out.prod_dictionary['astropy_table_product_ascii_list'] = tab_dp_list
            query_out.prod_dictionary['binary_data_product_list'] = bin_dp_list
            query_out.prod_dictionary['binary_image_product_list'] = bin_im_dp_list
            query_out.prod_dictionary['text_product_list'] = text_dp_list
            query_out.prod_dictionary['progress_product_list'] = progress_dp_list
        else:
            prod_name_list, file_name_list, image_list = [], [], []
            for product in prod_list.prod_list:
                product.write()
                try:
                    file_name_list.append(os.path.basename(product.file_path))
                except AttributeError:
                    pass
                im = product.get_html_draw()
                if im:
                    image_list.append(im)
                prod_name_list.append(product.name)

            query_out.prod_dictionary['file_name'] = file_name_list
            query_out.prod_dictionary['image'] = image_list[0] if len(image_list) == 1 else image_list
            query_out.prod_dictionary['name'] = prod_name_list
            if len(file_name_list) == 1:
                query_out.prod_dictionary['download_file_name'] = f'{file_name_list[0]}.gz'
            else:
                query_out.prod_dictionary['download_file_name'] = f'{self.backend_product_name}.tar.gz' 
            query_out.prod_dictionary['prod_process_message'] = ''

        return query_out
    
class NB2WInstrumentQuery(InstrumentQuery):
    def __init__(self, name, restricted_access):
        super().__init__(name, restricted_access=restricted_access)
        self._parameters_list = []
        self._build_par_dictionary()
