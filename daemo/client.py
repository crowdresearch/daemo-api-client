import requests


AUTH_ERROR = "Authentication credentials were not provided."

class DaemoClient:
    def __init__(self, host, client_id, access_token, refresh_token):
        self.host = host
        self.client_id = client_id
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.projects = dict()
        self.session = requests.session()

    def post_request(self, path, data):
        headers = {"Authorization": "Bearer " + self.access_token, "Content-Type": 'application/json'}
        response = self.session.post(self.host + path, data=data, headers=headers)
        if response.get('detail', '') == AUTH_ERROR:
            self.refresh()
            headers = {"Authorization": "Bearer " + self.access_token, "Content-Type": 'application/json'}
            response = self.session.post(self.host + path, data=data, headers=headers)
        return response

    def get_request(self, path):
        headers = {"Authorization": "Bearer " + self.access_token}
        response = self.session.get(url=self.host + path, headers=headers)
        if response.get('detail', '') == AUTH_ERROR:
            self.refresh()
            headers = {"Authorization": "Bearer " + self.access_token}
            response = self.session.get(url=self.host + path, headers=headers)
        return response

    def refresh(self):
        data = {'client_id': self.client_id, 'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token}
        response = self.session.post(self.host + 'api/oauth2-ng/token/', data=data)
        if 'error' in response:
            print "There was an error refreshing your access token."
            print "Please contact customer service or generate a new token on Daemo."
            exit()
        else:
            print "Successfully generated a new access token."
            self.access_token = response.get('access_token', '')
            self.refresh_token = response.get('refresh_token', '')

    def launch_task(self, project_id, data, accept, completion, stream=False):
        self.projects[project_id] = {'data': data, 'accept': accept, 'completion': completion, 'stream': stream}
        if stream:
            r = self.post_request(path='api/project/create-full/', data=data, stream=stream)
            for result in r: #as they come -- spawn subprocess to monitor events
                if result: #check to see if its actually something
                    action = accept(result)
                    if action:
                        completion(result)
                    #post action to take on the task
        else:
            while True:
                results = self.post_request(path='api/project/create-full/', data=data, stream=stream)
                #spawn and monitor as before except here we wait until server closes connection
                for result in results:
                    if result:
                        action = accept(result)
                        #post action to take on the task
            map(results, completion) #something like this

