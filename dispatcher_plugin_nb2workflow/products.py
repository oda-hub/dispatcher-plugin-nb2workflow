class NB2WProduct:
    def __init__(self, data, name = "nb2w"):
        self.data = data
        self.name = name
        
    def get_plot(self):
        pass

    @classmethod
    def prod_list_from_output_dict(cls, output_dict):
        mapping = {x.key: x for x in cls.__subclasses__()}
        
        prod_list = []
        for obj_name, obj_data in output_dict['result'].items():
            for data_type, data in obj_data.items():
                prod_list.append(
                    mapping[data_type](data, obj_name)
                )
        return prod_list
        
class NB2WLightCurveProduct(NB2WProduct):
    key = 'lightcurves'
    
class NB2WSpectrumProduct(NB2WProduct):
    key = 'spectrum'
    
class NB2WImageProduct(NB2WProduct):
    key = 'image'