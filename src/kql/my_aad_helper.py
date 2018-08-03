""" A module to acquire tokens from AAD.
"""

from datetime import timedelta, datetime
# import webbrowser
import dateutil.parser
from adal import AuthenticationContext
from kql.display  import Display
import getpass



class _MyAadHelper(object):
    def __init__(self, kusto_cluster, client_id=None, client_secret=None, username=None, password=None, authority=None):
        self.adal_context = AuthenticationContext('https://login.windows.net/{0}'.format(authority or 'microsoft.com'))
        self.code_adal_context = AuthenticationContext('https://login.windows.net/{0}'.format(authority or 'microsoft.com'))
        self.kusto_cluster = kusto_cluster
        self.client_id = client_id or "db662dc1-0cfe-4e1c-a843-19a68e65be58"
        self.client_secret = client_secret
        self.username = username
        self.password = password

    def acquire_token(self):
        """ A method to acquire tokens from AAD. """
        # print("my_aad_helper_acquire_token")
        token_response = self.adal_context.acquire_token(self.kusto_cluster, self.username, self.client_id)

        if token_response is not None:
            expiration_date = dateutil.parser.parse(token_response['expiresOn'])
            if expiration_date > datetime.utcnow() + timedelta(minutes=5):
                return token_response['accessToken']

        if self.client_secret is not None and self.client_id is not None:
            token_response = self.adal_context.acquire_token_with_client_credentials(
                self.kusto_cluster,
                self.client_id,
                self.client_secret)
        elif self.username is not None and self.password is not None:
            token_response = self.adal_context.acquire_token_with_username_password(
                self.kusto_cluster,
                self.username,
                self.password,
                self.client_id)
        else:
            code = self.adal_context.acquire_user_code(self.kusto_cluster, self.client_id)

            # Display.showInfoMessage(code['message'])

            # <button onclick="this.style.visibility='hidden';document.getElementById('user_code_p').innerHTML = '';myFunction()">Copy the above code, and Click to open authentication window</button>

            url = code['verification_url']
            html_str = """<!DOCTYPE html>
                <html><body>

                <h1 id="user_code_p"><b>""" +code["user_code"].strip()+ """</b><br></h1>

                <button id='my_button', onclick="this.style.visibility='hidden';kqlMagicCodeAuthFunction()">Copy the above code, and Click to open authentication window</button>

                <script>
                var kqlMagicUserCodeAuthWindow = null
                function kqlMagicCodeAuthFunction() {
                    var w = screen.width / 2;
                    var h = screen.height / 2;
                    params = 'width='+w+',height='+h
                    kqlMagicUserCodeAuthWindow = window.open('""" +url+ """', 'kqlMagicUserCodeAuthWindow', params);
                }
                </script>

                </body></html>"""

            Display.show(html_str)
            # webbrowser.open(code['verification_url'])
            try:
                token_response = self.adal_context.acquire_token_with_device_code(self.kusto_cluster, code, self.client_id)
            finally:
                html_str = """<!DOCTYPE html>
                    <html><body><script>

                        // close authentication window
                        if (kqlMagicUserCodeAuthWindow && kqlMagicUserCodeAuthWindow.opener != null && !kqlMagicUserCodeAuthWindow.closed) {
                            kqlMagicUserCodeAuthWindow.close()
                        }
                        // clear output cell 
                        Jupyter.notebook.clear_output(Jupyter.notebook.get_selected_index())

                    </script></body></html>"""

                Display.show(html_str)

        return token_response['accessToken']

