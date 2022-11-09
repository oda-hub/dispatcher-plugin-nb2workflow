import logging

from cdci_data_analysis.analysis.products import LightCurveProduct, BaseQueryProduct
from oda_api.data_products import NumpyDataProduct, ODAAstropyTable

logger = logging.getLogger(__name__)

# TODO: this should probably be defined in the main dispatcher code
class TableProduct(BaseQueryProduct):
    def __init__(self, name, table_data, file_name='table', **kwargs):
        self.table_data = table_data
        super().__init__(name, file_name=file_name, **kwargs)

    def encode(self):
        self.table_data.encode()

    def write(self, file_name=None, overwrite=True, format='fits', file_dir=None):
        file_path = self.file_path.get_file_path(file_name=file_name, file_dir=file_dir)
        self.table_data.write(file_path+'.fits', overwrite=overwrite, format=format)
        

class NB2WProduct:
    def __init__(self, encoded_data, data_product_type = BaseQueryProduct):
        self.name = encoded_data.get('name', 'nb2w')
        metadata = encoded_data.get('meta_data', {})
        numpy_data_prod = NumpyDataProduct.decode(encoded_data) # most products are NumpyDataProduct so default: 
                                                                # we follow BaseQueryProduct implementation
        self.dispatcher_data_prod = data_product_type(name = self.name, 
                                                     data= numpy_data_prod,
                                                     meta_data=metadata)
        
        
    def get_plot(self):
        pass
    
    @classmethod 
    def _init_as_list(cls, *args, **kwargs):
        return [cls(*args, **kwargs)]        

    @classmethod
    def prod_list_factory(cls, output_description_dict, output):
        mapping = {x.type_key: x for x in cls.__subclasses__()}
        
        prod_list = []
        for key in output_description_dict.keys():
            owl_type = output_description_dict[key]['owl_type']

            try:                        
                prod_list.extend( mapping.get(owl_type, cls)._init_as_list(output[key]) )
            except Exception as e:
                logger.warning('unable to construct %s product: %s from this: %s ', key, e, output[key])

        return prod_list

class NB2WAstropyTableProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#ODAAstropyTable'
    
    def __init__(self, encoded_data):
        self.name = encoded_data.get('name', 'nb2w')
        metadata = encoded_data.get('meta_data', {})
        table_data_prod = ODAAstropyTable.decode(encoded_data)
        self.dispatcher_data_prod = TableProduct(name = self.name, 
                                                 table_data = table_data_prod,
                                                 meta_data=metadata)
class NB2WLightCurveList(NB2WProduct):
    # TODO: change accorting to the new representation
    type_key = 'http://odahub.io/ontology#LightCurveList'
    
    @classmethod
    def _init_as_list(cls, encoded_obj):
        if type(encoded_obj) != list:
            raise ValueError('Wrong backend product structure')
        out_list = []
        for lc_dict in encoded_obj:
            out_list.append(NB2WLightCurveProduct(lc_dict))
        return out_list

class NB2WLightCurveProduct(NB2WProduct): 
    # TODO: relate to lightcurve product in cdci_data_analysis either by multiple inheritance or by composition
    type_key = 'http://odahub.io/ontology#LightCurve'
        
    def __init__(self, encoded_data):
        super().__init__(encoded_data, data_product_type = LightCurveProduct)
   
class NB2WSpectrumProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#Spectrum'
    
class NB2WImageProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#Image'