import requests



class WebSession():

    def __init__(self,credentials):
        self._credentials = credentials


    def get_default_headers(self):
        return {
            'Authorization': 'Basic ' + self._credentials,
            'Accept': 'application/json'}


    def create_session(self):
        session = requests.session()
        session.headers.update(self.get_default_headers())
        return session
