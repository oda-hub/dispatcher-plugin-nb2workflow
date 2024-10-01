from html.parser import HTMLParser
from functools import wraps
from cdci_data_analysis.analysis.ontology import Ontology
from json import dumps


class AstropyTableViewParser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        if tag == 'body':
            self.inbody = True
        if tag == 'script':
            self.inscript = True
        if getattr(self, 'intable', False):
            self.tabcode += self.get_starttag_text()
        if tag == 'table':
            self.intable = True
            attdic = dict(attrs)
            self.tabcode = f'<table class="{attdic["class"]} mmoda" id="{attdic["id"]}">'
 
            
    def handle_endtag(self, tag):
        if tag == 'body':
            self.inbody = False
        if tag == 'script':
            self.inscript = False
        if tag == 'table':
            self.intable = False
        if getattr(self, 'intable', False):
            self.tabcode += f"</{tag}>"
    
    def handle_data(self, data):
        if getattr(self, 'inbody', False) and getattr(self, 'inscript', False):
            self.script = data
        if getattr(self, 'intable', False):
            self.tabcode += data

class HashableDict(dict):
    def __hash__(self):
        return hash(dumps(self, sort_keys=True))

def with_hashable_dict(func):
    @wraps(func)
    def wrapper(*args, bk_descript_dict = {}, ontology_path = None):
        return func(*args, 
                    bk_descript_dict=HashableDict(bk_descript_dict), 
                    ontology_path=ontology_path)
    return wrapper


# TODO: this is better to move to oda_api.ontology_helper 
#       maybe implement generic get_hierarchy(self, uri, base_uri)
class OntologyMod(Ontology):
    def get_product_hierarchy(self, prod_uri):
        param_uri_m = f"<{prod_uri}>" if prod_uri.startswith("http") else prod_uri
        query = """
        select ?mid ( count(?mid2) as ?midcount ) where { 
        %s  (rdfs:subClassOf|a)* ?mid . 
        
        ?mid rdfs:subClassOf* ?mid2 .
        ?mid2 rdfs:subClassOf* oda:DataProduct .
        }
        group by ?mid
        order by desc(?midcount)
        """ % ( param_uri_m )

        qres = self.g.query(query)
        
        hierarchy = [str(row[0]) for row in qres]
        if len(hierarchy) > 0:
            return hierarchy  
        else:
            logger.warning("%s is not in ontology or not an oda:DataProduct", prod_uri)
            return [ prod_uri ]