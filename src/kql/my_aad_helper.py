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

            Display.showInfoMessage(code['message'])
            getpass.getpass(prompt = 'CTRL+C this code: {0}, then press enter to switch to authentication page'.format(code["user_code"]))

            url = code['verification_url']
            s  = '<script type="text/Javascript">'
            s += 'var win = window.open("' + url + '", "' + 'code' + '", "fullscreen=no, toolbar=no, location=no, directories=no, status=no, menubar=no, scrollbars=yes, resizable=no");'
            s += '</script>'
            html = Display.toHtml(body = s)

            Display.show(html)
            # webbrowser.open(code['verification_url'])
            token_response = self.adal_context.acquire_token_with_device_code(self.kusto_cluster, code, self.client_id)

        return token_response['accessToken']

