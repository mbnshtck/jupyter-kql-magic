import os.path
import re
import uuid
from IPython.core.display import display, HTML
from IPython.core.magics.display import Javascript





class Display(object):
    """
    """
    success_style = {'color': '#417e42', 'background-color': '#dff0d8', 'border-color': '#d7e9c6' }
    danger_style = {'color': '#b94a48', 'background-color': '#f2dede', 'border-color': '#eed3d7' }
    info_style = {'color': '#3a87ad', 'background-color': '#d9edf7', 'border-color': '#bce9f1' }
    warning_style = {'color': '#8a6d3b', 'background-color': '#fcf8e3', 'border-color': '#faebcc' }
    notebook_url = None

    @staticmethod
    def show(html_str, **kwargs):
        if len(html_str) > 0:
            if kwargs is not None and kwargs.get('fullscreen', False):
                html_str = Display._getHtmlFS(html_str, **kwargs)
                display(Javascript(html_str))
            else:
                # print(HTML(html_str)._repr_html_())
                display(HTML(html_str))

    @staticmethod
    def _getHtmlFS(html_str, **kwargs):
        if kwargs is not None and isinstance(kwargs.get('name'), str) and len(kwargs.get('name')) > 0:
            name = kwargs.get('name')
        else:
            name = uuid.uuid4().hex
        text_file = open(name + ".html", "w")
        text_file.write(html_str)
        text_file.close()
        url = Display._getServerUrl(name)
        # url = "https://www.w3schools.com"
        # print(url)
        # s = window.open("' + url + '", "' + name + '", "fullscreen=no, toolbar=no, location=no, directories=no, status=no, menubar=no, scrollbars=yes, resizable=yes,width=300,height=300");'
        return  'window.open("' + url + '", "' + name + '", "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no");'
        # s  = '<script type="text/Javascript">'
        # s += 'var win = window.open("' + url + '", "' + name + '", "toolbar=yes,scrollbars=yes,resizable=yes,top=500,left=500,width=400,height=400");'
        # s += '</script>'
        # return Display.toHtml(body = s)

    @staticmethod
    def _getServerUrl(name):
        # display(Javascript("""IPython.notebook.kernel.execute("NOTEBOOK_URL = '" + window.location + "'")"""))
        # print('NOTEBOOK_URL = {0}'.format(Display.notebook_url))
        parts =Display.notebook_url.split('/')
        parts.pop()
        parts.append(name)
        return '/'.join(parts) +  ".html"

    @staticmethod
    def toHtml(**kwargs):
        return """<html>
        <head>
        {0}
        </head>
        <body>
        {1}
        </body>
        </html>""".format(kwargs.get("head", ""), kwargs.get("body", ""))

    @staticmethod
    def _getMessageHtml(msg, palette):
        "get query information in as an HTML string"
        if isinstance(msg, list):
            msg_str = '<br>'.join(msg)
        elif isinstance(msg, str):
            msg_str = msg
        else:
            msg_str = str(msg)
        if len(msg_str) > 0:
            # success_style
            body =  "<div><p style='padding: 10px; color: {0}; background-color: {1}; border-color: {2}'>{3}</p></div>".format(
                palette['color'], palette['background-color'], palette['border-color'], msg_str.replace('\n', '<br>').replace(' ', '&nbsp'))
        else:
           body = ""
        return {"body" : body}


    @staticmethod
    def getSuccessMessageHtml(msg):
        return Display._getMessageHtml(msg, Display.success_style)

    @staticmethod
    def getInfoMessageHtml(msg):
        return Display._getMessageHtml(msg, Display.info_style)
            
    @staticmethod
    def getWarningMessageHtml(msg):
        return Display._getMessageHtml(msg, Display.warning_style)

    @staticmethod
    def getDangerMessageHtml(msg):
        return Display._getMessageHtml(msg, Display.danger_style)

    @staticmethod
    def showSuccessMessage(msg, **kwargs):
        html_str = Display.toHtml(**Display.getSuccessMessageHtml(msg))
        Display.show(html_str, **kwargs)

    @staticmethod
    def showInfoMessage(msg, **kwargs):
        html_str = Display.toHtml(**Display.getInfoMessageHtml(msg))
        Display.show(html_str, **kwargs)
            
    @staticmethod
    def showWarningMessage(msg, **kwargs):
        html_str = Display.toHtml(**Display.getWarningMessageHtml(msg))
        Display.show(html_str, **kwargs)

    @staticmethod
    def showDangerMessage(msg, **kwargs):
        html_str = Display.toHtml(**Display.getDangerMessageHtml(msg))
        Display.show(html_str, **kwargs)
