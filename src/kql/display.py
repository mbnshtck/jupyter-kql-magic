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
            if kwargs is not None and kwargs.get('window', False):
                file_name = Display._get_name(**kwargs)
                url = Display._html_to_url(html_str, file_name, **kwargs)
                Display.show_windows({file_name : url}, **kwargs)
            else:
                # print(HTML(html_str)._repr_html_())
                display(HTML(html_str))

    @staticmethod
    def show_windows(windows, **kwargs):
        # script = Display._get_window_script(url, name, **kwargs)
        # display(Javascript(script))
        html_str = Display._get_window_html(windows, **kwargs)
        display(HTML(html_str))

    @staticmethod
    def _html_to_url(html_str, file_name, **kwargs):
        text_file = open(file_name + ".html", "w")
        text_file.write(html_str)
        text_file.close()
        return Display._getServerUrl(file_name)

    @staticmethod
    def _get_name(**kwargs):
        if kwargs is not None and isinstance(kwargs.get('name'), str) and len(kwargs.get('name')) > 0:
            name = kwargs.get('name')
        else:
            name = uuid.uuid4().hex
        return name

    @staticmethod
    def _get_window_script(url, window_name, **kwargs):
        # print(url)
        # s = window.open("' + url + '", "' + name + '", "fullscreen=no, toolbar=no, location=no, directories=no, status=no, menubar=no, scrollbars=yes, resizable=yes,width=300,height=300");'
        # return  'window.open("' + url + '", "' + window_name + '", "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,width=500");'

        script = 'var win = window.open("", "' + window_name + '", "fullscreen=yes,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,width=500");'
        script += 'win.location ="' +url+'" ;win.focus();'
        # script = 'var win = window.open("' + url + '", "' + window_name + '", "fullscreen=yes,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,width=500");'
        # script += 'var w = screen.width; var h = screen.height; win.resizeTo(w/4, h); win.moveTo(50, 50);win.location ="' +url+'" ;win.focus();'
        return script
        # s  = '<script type="text/Javascript">'
        # s += 'var win = window.open("' + url + '", "' + name + '", "toolbar=yes,scrollbars=yes,resizable=yes,top=500,left=500,width=400,height=400");'
        # s += '</script>'

    @staticmethod
    def _get_window_html(windows, **kwargs):
        html_part1 = """<!DOCTYPE html>
            <html>
            <body>

            <button onclick="this.style.visibility='hidden';myFunction()">Click to open window</button>

            <script>
            function myFunction() {"""
            #var myWindow = window.open('""" +url+ """', '""" +window_name+ """', "width=200,height=100");
            #  // myWindow.document.write("<p>This window's name is: " + myWindow.name + "</p>");
        html_part3 = """
            }
            </script>

            </body>
            </html>"""
        html_part2 = ''
        for window_name in windows.keys():
            url = windows.get(window_name)
            html_part2 += window_name+ """ = window.open('""" +url+ """', '""" +window_name+ """', 'width=200,height=100');"""
        result =  html_part1 + html_part2 + html_part3
        # print(result)
        return result

    @staticmethod
    def _get_window_html_obsolete(url, window_name, **kwargs):
        html = """<!DOCTYPE html>
            <html>
            <body>

            <p>Click the button to create a window and then display the name of the new window.</p>

            <button onclick="myFunction()">Click to open window """ +window_name+ """</button>

            <script>
            function myFunction() {
                var myWindow = window.open('""" +url+ """', '""" +window_name+ """', "width=200,height=100");
                // myWindow.document.write("<p>This window's name is: " + myWindow.name + "</p>");
            }
            </script>

            </body>
            </html>"""
        return html


    @staticmethod
    def _getServerUrl(name):
        # display(Javascript("""IPython.notebook.kernel.execute("NOTEBOOK_URL = '" + window.location + "'")"""))
        # print('NOTEBOOK_URL = {0}'.format(Display.notebook_url))
        parts = Display.notebook_url.split('/')
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
