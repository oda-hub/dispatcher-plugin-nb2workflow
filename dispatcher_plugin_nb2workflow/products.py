from multiprocessing.sharedctypes import Value


class NB2WProduct:
    def __init__(self, data, name = "nb2w"):
        self.data = data
        self.name = name
        
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
            prod_list.extend( mapping.get(owl_type, cls)._init_as_list(output[key], output_description_dict[key]['name']) )
        return prod_list

class NB2WLightCurveList(NB2WProduct):
    type_key = 'http://odahub.io/ontology#LightCurveList'
    
    @classmethod
    def _init_as_list(cls, encoded_obj, name = None):
        if type(encoded_obj) != list:
            raise ValueError('Wrong backend product structure')
        out_list = []
        for lc_dict in encoded_obj:
            name = lc_dict.get('metadata', {}).get('name', f"{lc_dict.get('metadata', 'lightcurve')}")
            out_list.append(NB2WLightCurveProduct(lc_dict, name))
        return out_list

class NB2WLightCurveProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#LightCurve'
   
class NB2WSpectrumProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#Spectrum'
    
class NB2WImageProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#Image'