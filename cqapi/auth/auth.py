import attr
import datetime
import requests
from IPython.display import Javascript, display
from importlib.resources import open_text, path
import logging


def keycloak_cmd2(auth_url: str, client_name: str):
    return f"""
    // get keycloak module from auth server
    require(["{auth_url}/js/keycloak.js"], function(){{

        // save notebook, because we will leave the page to authenticate
        document.execCommand("SaveAs");

        // initiate keycloak object
        var keycloak_obj = new Keycloak({{
            "realm": "Ingef",
            "auth-server-url": "{auth_url}",
            "ssl-required": "external",
            "resource": "{client_name}",
            "clientId": "{client_name}",
            "public-client": true,
            "confidential-port": 0
        }})
        
        console.log("Test Before");
        // let user log in
        keycloak_obj.init({{onLoad: 'login-required'}}).then(function(authenticated) {{
            // when login was successful we let the user know
            alert("test");
            alert(authenticated ? 'Login war erfolgreich' : 'LogIn ist fehlgeschlagen');
            console.log("Test after alter");
            console.log(keycloak_obj.token);
            var assign_token_to_conquery_cmd = "eva.update_token_handler(token = "token";
            console.log(assign_token_to_conquery_cmd);
            var kernel = IPython.notebook.kernel;
            kernel.execute(assign_token_to_conquery_cmd);
        }}).catch(function() {{
            alert('Initialisierung der Authentifizierung ist fehlgeschlagen. Bitte wenden Sie sich an das EVA-Team');
        }});
        
        console.log("Test After");
        console.log(keycloak_obj.token)     
    }})
    """


@attr.s(kw_only=True, auto_attribs=True)
class TokenHandler:
    auth_url: str
    client_name: str
    token: str
    is_simple_token_handler: bool
    refresh_token: str = attr.ib(default="", init=False)
    last_refresh_time: datetime.datetime = attr.ib(default=datetime.datetime.now(), init=False)
    refresh_time_seconds: int = attr.ib(default=300, init=False)

    """
    def refresh_access_token(self):
        if self.is_simple_token_handler:
            return

        response = requests.post(url=f"{self.auth_url}/realms/Ingef/protocol/openid-connect/token",
                                 data={"grant_type": "refresh_token",
                                       "refresh_token": self.refresh_token,
                                       "client_id": self.client_name})

        response.raise_for_status()

        response_dict: dict = response.json()

        self.token = response_dict["access_token"]
        self.refresh_token = response_dict["refresh_token"]

    def check_token(self):
        if self.is_simple_token_handler:
            return

        time_difference: datetime.timedelta = datetime.datetime.now() - self.last_refresh_time

        if time_difference.seconds > self.refresh_time_seconds:
            self.refresh_access_token()
            self.last_refresh_time = datetime.datetime.now()
    
    """
    def get_token(self):
        #self.check_token()
        return self.token

    def login(self):
        if self.is_simple_token_handler:
            return

        #with path("cqapi.auth", "run_keycloak.js") as template_file_path:
        run_keycloak_script: str = open_text("cqapi.auth", "run_keycloak.js").read()
        return Javascript(run_keycloak_script)

        #return display(Javascript(keycloak_cmd(auth_url=self.auth_url,
        #                                       client_name=self.client_name)))


"""
th = TokenHandler(auth_url="localhost:8000/auth/",
                  client_name="conquery-release",
                  token = "token",
                  is_simple_token_handler=False)

th.refresh_token = "token"

th.refresh_access_token()
"""
# print(keycloak_cmd(auth_url="test", client_name="test"))
