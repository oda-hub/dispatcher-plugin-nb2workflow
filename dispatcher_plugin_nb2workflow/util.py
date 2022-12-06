from html.parser import HTMLParser

class AstropyTableViewParser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        if tag == 'body':
            self.inbody = True
        if tag == 'script':
            self.inscript = True
        if tag == 'table':
            self.intable = True
            self.tabcode = ''
        if getattr(self, 'intable', False):
            self.tabcode += self.get_starttag_text()
            
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