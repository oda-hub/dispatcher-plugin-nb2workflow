from html.parser import HTMLParser

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