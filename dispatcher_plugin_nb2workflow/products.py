import logging
import os
import json

from cdci_data_analysis.analysis.products import LightCurveProduct, BaseQueryProduct, ImageProduct, SpectrumProduct
from oda_api.data_products import NumpyDataProduct, ODAAstropyTable, BinaryData, PictureProduct
from .util import AstropyTableViewParser
from io import StringIO

logger = logging.getLogger(__name__)

# TODO: this should probably be defined in the main dispatcher code
class TableProduct(BaseQueryProduct):
    def __init__(self, name, table_data, file_dir = './', **kwargs):
        self.table_data = table_data
        fname = name if name.endswith('csv') else f"{name}.ecsv"
        super().__init__(name, file_name=fname, file_dir = file_dir, **kwargs)

    def encode(self):
        self.table_data.encode()

    def write(self, file_name=None, overwrite=True, file_dir=None):
        if file_name:
            file_path = self.file_path.get_file_path(file_name=file_name, file_dir=file_dir)
        else:
            file_path = self.file_path.path
            
        self.table_data.write(file_path, overwrite=overwrite, format='ascii.ecsv')
        
class NB2WProduct:
    def __init__(self, encoded_data, data_product_type = BaseQueryProduct, out_dir = None, name = 'nb2w'):
        encoded_data = self._dejsonify(encoded_data)
        self.name = encoded_data.get('name', name)
        metadata = encoded_data.get('meta_data', {})
        self.out_dir = out_dir
        numpy_data_prod = NumpyDataProduct.decode(encoded_data) # most products are NumpyDataProduct so default: 
                                                                # we follow BaseQueryProduct implementation
        self.dispatcher_data_prod = data_product_type(name = self.name, 
                                                     data= numpy_data_prod,
                                                     meta_data=metadata,
                                                     file_dir = out_dir,
                                                     file_name = f"{self.name}")
    
    def write(self):
        file_path = self.dispatcher_data_prod.file_path
        self.dispatcher_data_prod.write()
        self.file_path = file_path.path
    
    def get_html_draw(self):
        try:
            return self.dispatcher_data_prod.get_html_draw()
        except:
            return {'image': {'div': '<br>No preview available', 'script': ''} }
        
    
    @classmethod 
    def _init_as_list(cls, *args, **kwargs):
        return [cls(*args, **kwargs)]        

    @classmethod
    def prod_list_factory(cls, output_description_dict, output, out_dir = None):
        mapping = {x.type_key: x for x in cls.__subclasses__()}
        
        prod_list = []
        for key in output_description_dict.keys():
            owl_type = output_description_dict[key]['owl_type']

            try:                 
                prod_list.extend( mapping.get(owl_type, cls)._init_as_list(output[key], out_dir = out_dir, name = key) )
            except Exception as e:
                logger.warning('unable to construct %s product: %s from this: %s ', key, e, output[key])

        return prod_list

    @staticmethod
    def _dejsonify(encoded_data):
        if isinstance(encoded_data, str):
            return json.loads(encoded_data)
        return encoded_data
class NB2WBinaryProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#ODABinaryProduct'
    
    def __init__(self, encoded_data, out_dir = None, name = 'bindata'):
        self.out_dir = out_dir
        self.name = name
        self.dispatcher_data_prod = encoded_data
    
    def write(self):
        file_path = os.path.join(self.out_dir, self.name)
        bin_data = BinaryData().decode(self.dispatcher_data_prod)
        with open(file_path, 'wb') as fd:
            fd.write(bin_data)
        self.file_path = file_path

class NB2WTextProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#ODATextProduct'
    
    def __init__(self, text_data, out_dir = None, name = 'text'):
        self.out_dir = out_dir
        self.name = name
        if isinstance(text_data, str):
            self.dispatcher_data_prod = text_data
        else:
            raise ValueError(f'This is not a string: {text_data}')
        
    def write(self):
        file_path = os.path.join(self.out_dir, self.name)
        with open(file_path, 'w') as fd:
            fd.write(self.dispatcher_data_prod)
        self.file_path = file_path
        
    def get_html_draw(self):
        return {'image': {'div': '<br>'+self.dispatcher_data_prod, 'script': ''} }

class NB2WPictureProduct(NB2WProduct): 
    type_key = 'http://odahub.io/ontology#ODAPictureProduct'  
    
    def __init__(self, encoded_data, out_dir = None, name = 'picture'):
        self.out_dir = out_dir
        # NOTE: no dispatcher data product class here. (As well as in binary/text data)
        # Currently not needed but may be usefult later to generate fromtend representation etc.
        self.dispatcher_data_prod = PictureProduct.decode(encoded_data)
        fname = getattr(self.dispatcher_data_prod, 'file_path', None)
        if fname is None:
            fname = name
        self.name = os.path.basename(fname)

    def write(self):
        file_path = os.path.join(self.out_dir, self.name)
        self.dispatcher_data_prod.write_file(file_path)
        self.file_path = file_path

    def get_html_draw(self):
        enc = self.dispatcher_data_prod.encode()
        b64_dat = enc["b64data"].replace("-", "+").replace("_", "/") 
        return {'image': {'div': f'<br><img src="data:image/{enc["img_type"]};base64,{b64_dat}">', 
                          'script': ''} }

class NB2WAstropyTableProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#ODAAstropyTable'
    
    def __init__(self, encoded_data, out_dir = None, name = 'astropy_table'):
        encoded_data = self._dejsonify(encoded_data)
        self.name = encoded_data.get('name', name)
        metadata = encoded_data.get('meta_data', {})
        self.out_dir = out_dir
        table_data_prod = ODAAstropyTable.decode(encoded_data)
        self.dispatcher_data_prod = TableProduct(name = self.name, 
                                                 table_data = table_data_prod,
                                                 meta_data=metadata,
                                                 file_dir = out_dir)
    
    def get_html_draw(self):
        with StringIO() as sio:
            self.dispatcher_data_prod.table_data.write(sio, format='jsviewer')
            sio.seek(0)
            html_text = sio.read()
        
        parser = AstropyTableViewParser()
        parser.feed(html_text)
        
        script_text = parser.script.replace('$(document).ready', '').replace('$(', 'jQuery(').rpartition(');')[0] + ')();'
        
        return {'image': {'div': '<br><br>'+parser.tabcode,
                          'script': f"<script>{script_text}</script>"} }
        
class NB2WLightCurveList(NB2WProduct):
    type_key = 'http://odahub.io/ontology#LightCurveList'
    
    @classmethod
    def _init_as_list(cls, encoded_obj, out_dir = None, name = 'lc_list'):
        if type(encoded_obj) != list:
            raise ValueError('Wrong backend product structure')
        out_list = []
        for nn, lc_dict in enumerate(encoded_obj):
            out_list.append(NB2WLightCurveProduct(lc_dict, out_dir = out_dir, name = f"{name}_{nn}"))
        return out_list

class NB2WLightCurveProduct(NB2WProduct): 
    type_key = 'http://odahub.io/ontology#LightCurve'
        
    def __init__(self, encoded_data, out_dir = None, name = 'lc'):
        super().__init__(encoded_data, data_product_type = LightCurveProduct, out_dir = out_dir, name = name)
        
    def get_html_draw(self):
        unit_ID=1 # TODO: it could be optional
        du = self.dispatcher_data_prod.data.data_unit[unit_ID]
        data, header, units = du.data, du.header, du.units_dict
        data_col = data.dtype.names[1]
        err_col = None
        if len(data.dtype.names) == 3 and data.dtype.names[2].startswith('ERR'):
            err_col = data.dtype.names[2]
        x_label = f"TIME, {units['TIME']}"
        y_units = getattr(units, data_col, None)
        y_label = f"{data_col}, {y_units}" if y_units else data_col 
        
        im_dic = self.dispatcher_data_prod.get_html_draw(x = data['TIME'], 
                                                         y=data[data_col], 
                                                         dy=data[err_col] if err_col else None,
                                                         x_label = x_label,
                                                         y_label = y_label
                                                         )
        return im_dic
   
class NB2WSpectrumProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#Spectrum'
    
    def __init__(self, encoded_data, out_dir=None, name = 'spec'):
        super().__init__(encoded_data, data_product_type=SpectrumProduct, out_dir=out_dir, name = name)
        
class NB2WImageProduct(NB2WProduct):
    type_key = 'http://odahub.io/ontology#Image'
    
    def __init__(self, encoded_data, out_dir = None, name = 'image'):
        super().__init__(encoded_data, data_product_type = ImageProduct, out_dir = out_dir, name = name)
