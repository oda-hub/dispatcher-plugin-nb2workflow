from cdci_data_analysis.analysis.queries import ProductQuery, QueryOutput, BaseQuery
from cdci_data_analysis.analysis.parameters import Name, Integer, Float, Time, Angle
from .products import NB2WProduct
from .dataserver_dispatcher import NB2WDataDispatcher

class NB2WProductQuery(ProductQuery): 
    def __init__(self, name, backend_product_name, backend_param_dict):
        self.backend_product_name = backend_product_name
        
        src_query_pars = ['src_name', 'RA', 'DEC', 'T1', 'T2']
        plist = []
        for pname, pval in backend_param_dict.items():
            # TODO: find a better way to deal with common parameters and not rely on parameter names
            if pname in src_query_pars:
                continue
            
            # FIXME: demo only, advanced correspondance needed
            elif pval['python_type']['type_object'] == "<class 'str'>":
                plist.append(Name(value=pval['default_value'], name=pname))
            elif pval['python_type']['type_object'] == "<class 'int'>":
                plist.append(Integer(value=pval['default_value'], name=pname))
            elif pval['python_type']['type_object'] == "<class 'float'>":
                plist.append(Float(value=pval['default_value'], name=pname))    
            else:
                raise NotImplementedError('unknown type of parameter')
            
        super().__init__(name, parameters_list = plist)
    
    @classmethod
    def query_list_and_dict_factory(cls, data_server_url):
        backend_options = NB2WDataDispatcher.query_backend_options(data_server_url)
        product_names = backend_options.keys()
        qlist = []
        qdict = {}
        for product_name in product_names:
            backend_param_dict = backend_options[product_name]['parameters']
            qlist.append(cls(f'{product_name}_query', product_name, backend_param_dict))
            qdict[product_name] = f'{product_name}_query'
        return qlist, qdict
        
        
    def get_data_server_query(self, instrument, config=None, **kwargs):
        param_dict = {}
        for param_name in instrument.get_parameters_name_list():
            param_dict[param_name] = instrument.get_par_by_name(param_name).value
        
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
        prod_list = NB2WProduct.prod_list_from_output_dict(_o_dict['output'])
        return prod_list
    
    def process_product_method(self, instrument, prod_list, api=False):
        query_out = QueryOutput()
        image_prod  = prod_list.prod_list[0]

        if api is True:
            raise NotImplementedError
        else:
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
