from cdci_data_analysis.analysis.queries import ProductQuery, QueryOutput, BaseQuery, SourceQuery
from cdci_data_analysis.analysis.parameters import Parameter, Name
from .products import NB2WProduct, NB2WAstropyTableProduct, NB2WBinaryProduct, NB2WPictureProduct, NB2WTextProduct
from .dataserver_dispatcher import NB2WDataDispatcher
import os

def construct_parameter_lists(backend_param_dict):
    src_query_pars_uris = { "http://odahub.io/ontology#PointOfInterestRA": "RA",
                            "http://odahub.io/ontology#PointOfInterestDEC": "DEC",
                            "http://odahub.io/ontology#StartTime": "T1",
                            "http://odahub.io/ontology#EndTime": "T2",
                            "http://odahub.io/ontology#AstrophysicalObject": "src_name"}
    par_name_substitution = {}
    
    plist = []
    source_plist = []
    for pname, pval in backend_param_dict.items():
        if pval['owl_type'] in src_query_pars_uris.keys():
            default_pname = src_query_pars_uris[pval['owl_type']]
            par_name_substitution[ default_pname ] = pname
            source_plist.append(Parameter.from_owl_uri(pval['owl_type'], 
                                                       value=pval['default_value'], 
                                                       name=default_pname,
                                                       Time_format_name='T_format'))
        else:
            plist.append(Parameter.from_owl_uri(pval['owl_type'], 
                                                value=pval['default_value'], 
                                                name=pname,
                                                Time_format_name='T_format'))
    
    return {'source_plist': source_plist,
            'prod_plist': plist,
            'par_name_substitution': par_name_substitution}

class NB2WSourceQuery(BaseQuery):
    @classmethod
    def from_backend_options(cls, backend_options):
        product_names = backend_options.keys()
        # Note that different backend products could contain different sets of the source query parameters. 
        # So we squash them into one list without duplicates
        parameters_dict = {}
        for product_name in product_names:
            backend_param_dict = backend_options[product_name]['parameters']
            prod_source_plist = construct_parameter_lists(backend_param_dict)['source_plist']
            for par in prod_source_plist:
                parameters_dict[par.name] = par
        parameters_list = list(parameters_dict.values())
        parameters_list.append(Name(name_format='str', name='token', value=None))
        return cls('src_query', parameters_list)

class NB2WProductQuery(ProductQuery): 
    def __init__(self, name, backend_product_name, backend_param_dict, backend_output_dict):
        self.backend_product_name = backend_product_name
        self.backend_output_dict = backend_output_dict
        parameter_lists = construct_parameter_lists(backend_param_dict)
        self.par_name_substitution = parameter_lists['par_name_substitution']
        plist = parameter_lists['prod_plist']
        super().__init__(name, parameters_list = plist)
    
    @classmethod
    def query_list_and_dict_factory(cls, backend_options):
        product_names = backend_options.keys()
        qlist = []
        qdict = {}
        for product_name in product_names:
            backend_param_dict = backend_options[product_name]['parameters']
            backend_output_dict = backend_options[product_name]['output']
            qlist.append(cls(f'{product_name}_query', product_name, backend_param_dict, backend_output_dict))
            qdict[product_name] = f'{product_name}_query'
        return qlist, qdict
        
        
    def get_data_server_query(self, instrument, config=None, **kwargs):
        param_dict = {}
        for param_name in instrument.get_parameters_name_list():
            param_dict[self.par_name_substitution.get(param_name, param_name)] = instrument.get_par_by_name(param_name).value
        
        return instrument.data_server_query_class(instrument=instrument,
                                                config=config,
                                                param_dict=param_dict,
                                                task=self.backend_product_name) 
    
    def build_product_list(self, instrument, res, out_dir, api=False):
        prod_list = []
        if out_dir is None:
            out_dir = './'
        if 'output' in res.json().keys(): # in synchronous mode
            _o_dict = res.json() 
        else:
            _o_dict = res.json()['data']
        prod_list = NB2WProduct.prod_list_factory(self.backend_output_dict, _o_dict['output'], out_dir) 
        return prod_list
    
    def process_product_method(self, instrument, prod_list, api=False):
        query_out = QueryOutput()
        
        
        np_dp_list, bin_dp_list, tab_dp_list, bin_im_dp_list, text_dp_list = [], [], [], [], []
        if api is True:
            for product in prod_list.prod_list:
                if isinstance(product, NB2WAstropyTableProduct):
                    tab_dp_list.append(product.dispatcher_data_prod.table_data)
                elif isinstance(product, NB2WBinaryProduct):
                    bin_dp_list.append(product.dispatcher_data_prod)
                elif isinstance(product, NB2WPictureProduct):
                    bin_im_dp_list.append(product.dispatcher_data_prod) 
                elif isinstance(product, NB2WTextProduct):
                    text_dp_list.append(product.dispatcher_data_prod)
                else: # NB2WProduct contains NumpyDataProd by default
                    np_dp_list.append(product.dispatcher_data_prod.data)
                    
            query_out.prod_dictionary['numpy_data_product_list'] = np_dp_list
            query_out.prod_dictionary['astropy_table_product_ascii_list'] = tab_dp_list
            query_out.prod_dictionary['binary_data_product_list'] = bin_dp_list
            query_out.prod_dictionary['binary_image_product_list'] = bin_im_dp_list
            query_out.prod_dictionary['text_product_list'] = text_dp_list
        else:
            prod_name_list, file_name_list, image_list = [], [], []
            for product in prod_list.prod_list:
                product.write()
                file_name_list.append(os.path.basename(product.file_path))
                im = product.get_html_draw()
                if im:
                    image_list.append(im)
                prod_name_list.append(product.name)

            query_out.prod_dictionary['file_name'] = file_name_list
            query_out.prod_dictionary['image'] = image_list
            query_out.prod_dictionary['name'] = prod_name_list
            
            query_out.prod_dictionary['download_file_name'] = 'foo.tar.gz' # TODO:
            query_out.prod_dictionary['prod_process_message'] = ''

        return query_out
    
class NB2WInstrumentQuery(BaseQuery):
    def __init__(self, name):
        self.input_prod_list_name = None # this is a workaround
        super().__init__(name, [])
