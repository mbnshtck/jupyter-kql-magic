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
    showfiles_base_url = None
    showfiles_base_path = None
    showfiles_folder_name = None
    notebooks_host = None

    @staticmethod
    def show(html_str, **kwargs):
        if len(html_str) > 0:
            if kwargs is not None and kwargs.get('popup_window', False):
                file_name = Display._get_name(**kwargs)
                file_path = Display._html_to_file_path(html_str, file_name, **kwargs)
                Display.show_window(file_name, file_path, kwargs.get('botton_text'), **kwargs)
            else:
                # print(HTML(html_str)._repr_html_())
                display(HTML(html_str))

    @staticmethod
    def show_window(window_name, file_path, button_text = None, **kwargs):
        html_str = Display._get_window_html(window_name,file_path, button_text, **kwargs)
        display(HTML(html_str))

    @staticmethod
    def _html_to_file_path(html_str, file_name, **kwargs):
        full_file_name = Display.showfiles_base_path +file_name+ ".html"
        text_file = open(full_file_name, "w")
        text_file.write(html_str)
        text_file.close()
        # ipython will delete file at shutdown or by restart
        get_ipython().tempfiles.append(full_file_name)
        file_path = Display.showfiles_folder_name + '/' +file_name+ ".html"
        return file_path

    @staticmethod
    def _get_name(**kwargs):
        if kwargs is not None and isinstance(kwargs.get('name'), str) and len(kwargs.get('name')) > 0:
            name = kwargs.get('name')
        else:
            name = uuid.uuid4().hex
        return name

    @staticmethod
    def _get_window_html(window_name, file_path, button_text = None, **kwargs):
        notebooks_host = Display.notebooks_host or ''
        button_text = button_text or 'popup window'
        window_params = "fullscreen=no,directories=no,location=no,menubar=no,resizable=yes,scrollbars=yes,status=no,titlebar=no,toolbar=no,"
        html_str = """<!DOCTYPE html>
            <html><body>

            <button onclick="this.style.visibility='hidden';kqlMagicLaunchWindowFunction('"""+file_path+"""','""" +window_params+ """','""" +window_name+ """','"""+notebooks_host+"""')">""" +button_text+ """</button>

            <script>

            function kqlMagicLaunchWindowFunction(file_path, window_params, window_name, notebooks_host) {
                var url;
                if (file_path.startsWith('http')) {
                    url = file_path;
                } else {
                    var base_url = '';

                    var azure_host = (notebooks_host == null || notebooks_host.length == 0) ? 'https://notebooks.azure.com' : notebooks_host;
                    var start = azure_host.search('//');
                    var azure_host_suffix = '.' + azure_host.substring(start+2);

                    var loc = String(window.location);
                    var end = loc.search(azure_host_suffix);
                    start = loc.search('//');
                    if (start > 0 && end > 0) {
                        var parts = loc.substring(start+2, end).split('-');
                        if (parts.length == 2) {
                            var library = parts[0];
                            var user = parts[1];
                            base_url = azure_host + '/api/user/' +user+ '/library/' +library+ '/html';
                        }
                    }
                    if (base_url.length == 0) {
                        var parts = loc.split('/');
                        parts.pop();
                        base_url = parts.join('/');
                    }
                    url = base_url + '/' + file_path;
                }

                window.focus();
                var w = screen.width / 2;
                var h = screen.height / 2;
                params = 'width='+w+',height='+h;
                kqlMagic_""" +window_name+ """ = window.open(url, window_name, window_params + params);
            }
            </script>

            </body></html>"""
        # print(html_str)
        return html_str

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
            msg_str = msg_str.replace('"', '&quot;').replace("'", '&apos;').replace('\n', '<br>').replace(' ', '&nbsp')
            body =  "<div><p style='padding: 10px; color: {0}; background-color: {1}; border-color: {2}'>{3}</p></div>".format(
                palette['color'], palette['background-color'], palette['border-color'], msg_str)
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
