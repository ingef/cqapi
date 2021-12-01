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
            var assign_token_to_eva_cmd = "eva.update_token_handler(token='" + keycloak_obj.token + "', refresh_token='" + keycloak_obj.refreshToken + "')";
            console.log(assign_token_to_eva_cmd);
            var kernel = IPython.notebook.kernel;
            kernel.execute(assign_token_to_eva_cmd);
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
th = TokenHandler(auth_url="https://auth.ingef.de/auth/",
                  client_name="eva-release",
                  token="eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJFYU1zSTBRTUJQakJkZ0FFSF90cGVzZ0VXRlFrdEFNYThCejVSelFLYllrIn0.eyJleHAiOjE2MzgyODk3NDAsImlhdCI6MTYzODI4OTQ0MCwiYXV0aF90aW1lIjoxNjM4Mjc4NjM5LCJqdGkiOiI3ZWQzZjA4MS1mNTAwLTQxYzItOWZlMy00MmVjNjRhNWZiYmUiLCJpc3MiOiJodHRwczovL2F1dGguaW5nZWYuZGUvYXV0aC9yZWFsbXMvSW5nZWYiLCJhdWQiOiJldmEtcHJvZCIsInN1YiI6ImJiYmM2ZGEwLWI3YTMtNDFjOC1hODNjLTQ4YjU1ODA0ZWY0YSIsInR5cCI6IkJlYXJlciIsImF6cCI6ImV2YS1wcm9kIiwibm9uY2UiOiI1NTc3ZjBlNS0xODk4LTQxMDItYTYyOS0yZjkyOTE3ZDM5NDEiLCJzZXNzaW9uX3N0YXRlIjoiMDk3NzU1MmUtMzA2My00M2Q0LWE3NDQtZjA4NDZiYzY3NDZmIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwczovL2V2YS5pbmdlZi5kZSJdLCJzY29wZSI6Im9wZW5pZCBlbWFpbCBwcm9maWxlIiwic2lkIjoiMDk3NzU1MmUtMzA2My00M2Q0LWE3NDQtZjA4NDZiYzY3NDZmIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsIm5hbWUiOiJNYXggS25vYmxvY2giLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJtYXgua25vYmxvY2giLCJnaXZlbl9uYW1lIjoiTWF4IiwiZmFtaWx5X25hbWUiOiJLbm9ibG9jaCIsImVtYWlsIjoibWF4Lmtub2Jsb2NoQGluZ2VmLmRlIn0.IyXnUfepBZ8FqEmeARH14ykjTldb073IedmuPhEeWZb2X_YBnlqiDHisaL5lfeEF5_p8PSPrNc94rFVMRjt4r498n10btyFoSaoUvwYwq9-8HJ4gkNkzepWkvAduIBm3NeE3ZM1toSXaTqGqTaoiVVVkJCdVp-tjHfsmYB5Z7fc0KeMdibcgMg4ZO-ndcL3T3hpDI8oNfHj9YTje1euHzEs6vX4wZ5dTmR3ckm5Yd0fWopfX71o0ysjXD9SKW8TE2KvhERC3mcQCFRE08WH8k5_oWaE7tqtdQzmtDHCpZhLJDo7L_Ql3LoWdWAnaWc0Pnk_9_LG0IJF_r2P-jEkgWw",
                  is_simple_token_handler=False)

th.refresh_token = "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIzM2ZiZWY5Yi0xNzljLTRhN2MtYTBlYi0zMjk0NzExYjU3NGEifQ.eyJleHAiOjE2MzgyOTEyNDAsImlhdCI6MTYzODI4OTQ0MCwianRpIjoiMTYxODEyMTEtN2NlNi00OTUwLTk1NjktYjk2MzczMmExYzlhIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLmluZ2VmLmRlL2F1dGgvcmVhbG1zL0luZ2VmIiwiYXVkIjoiaHR0cHM6Ly9hdXRoLmluZ2VmLmRlL2F1dGgvcmVhbG1zL0luZ2VmIiwic3ViIjoiYmJiYzZkYTAtYjdhMy00MWM4LWE4M2MtNDhiNTU4MDRlZjRhIiwidHlwIjoiUmVmcmVzaCIsImF6cCI6ImV2YS1wcm9kIiwibm9uY2UiOiI1NTc3ZjBlNS0xODk4LTQxMDItYTYyOS0yZjkyOTE3ZDM5NDEiLCJzZXNzaW9uX3N0YXRlIjoiMDk3NzU1MmUtMzA2My00M2Q0LWE3NDQtZjA4NDZiYzY3NDZmIiwic2NvcGUiOiJvcGVuaWQgZW1haWwgcHJvZmlsZSIsInNpZCI6IjA5Nzc1NTJlLTMwNjMtNDNkNC1hNzQ0LWYwODQ2YmM2NzQ2ZiJ9.Tg2p1q-P3CjzYzG-JmFDXjCd4IJP84xOEM51lA80bHA"

th.refresh_access_token()
"""
# print(keycloak_cmd(auth_url="test", client_name="test"))
