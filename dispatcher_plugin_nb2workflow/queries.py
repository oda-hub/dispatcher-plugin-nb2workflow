from cdci_data_analysis.analysis.queries import ProductQuery, QueryOutput, BaseQuery
from cdci_data_analysis.analysis.parameters import Parameter
from .products import NB2WProduct
from .dataserver_dispatcher import NB2WDataDispatcher

class NB2WProductQuery(ProductQuery): 
    def __init__(self, name, backend_product_name, backend_param_dict, backend_output_dict):
        self.backend_product_name = backend_product_name
        self.backend_output_dict = backend_output_dict
        
        std_query_pars_uris = {"http://odahub.io/ontology#PointOfInterestRA": "RA",
                               "http://odahub.io/ontology#PointOfInterestDEC": "DEC",
                               "http://odahub.io/ontology#StartTime": "T1",
                               "http://odahub.io/ontology#EndTime": "T2",
                               "http://odahub.io/ontology#AstrophysicalObject": "src_name"}
        self.par_name_substitution = {}
        
        plist = []
        for pname, pval in backend_param_dict.items():
            if pval['owl_type'] in std_query_pars_uris.keys():
                self.par_name_substitution[ std_query_pars_uris[pval['owl_type']] ] = pname
                # TODO: probably set default values of these parameters from backend
            else:
                plist.append(Parameter.from_owl_uri(pval['owl_type'], value=pval['default_value'], name=pname))
                
        super().__init__(name, parameters_list = plist)
    
    @classmethod
    def query_list_and_dict_factory(cls, data_server_url):
        backend_options = NB2WDataDispatcher.query_backend_options(data_server_url)
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
        prod_list = NB2WProduct.prod_list_factory(self.backend_output_dict, _o_dict['output']) 
        return prod_list
    
    def process_product_method(self, instrument, prod_list, api=False):
        query_out = QueryOutput()
        
        if api is True:
            np_dp_list = []
            for product in prod_list.prod_list:
                np_dp_list.append(product.dispatcher_data_prod.data)
            query_out.prod_dictionary['numpy_data_product_list'] = np_dp_list
            query_out.prod_dictionary['binary_data_product_list'] = []
        else:
            raise NotImplementedError
            plot_dict = {'image': image_prod.get_plot()}
            #image_prod.write() 

            query_out.prod_dictionary['name'] = image_prod.name
            query_out.prod_dictionary['file_name'] = 'foo' 
            query_out.prod_dictionary['image'] = plot_dict
            query_out.prod_dictionary['download_file_name'] = 'bar.tar.gz'
            query_out.prod_dictionary['prod_process_message'] = ''

        return query_out
    
class NB2WInstrumentQuery(BaseQuery):
    def __init__(self, name):
        self.input_prod_list_name = None # this is a workaround
        super().__init__(name, [])
