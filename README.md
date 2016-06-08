# daemo-api-client

Get Oauth2 token (valid for 10 hours)
`curl -X POST -d "username=yourUsername&password=yourPassword" https://HOSTNAME/api/user/authenticate/`

you will get a response like this:
```
{"username":"dmorina","first_name":"Durim","last_name":"Morina","is_worker":true,"last_login":"2016-04-26T01:49:39.090Z","client_id":"99pN6E4fEdnXfO0QocihEHrPsUpPgMJ9GbFPEcb8","client_secret":"fRsc1Q9BZU8NXICnL3nxf770QLajOrCRskCkl6lVpn6S7qFVNL9z1YBebW0QTphTPb6uuaaEv0MtzvU7VunoslyEq2PdKLHvivueZmvxmw6GR0oe8N2oJRzB8QoZkxJd","is_requester":true,"email":"drm.mrn@gmail.com","date_joined":"2016-01-19T18:24:50.695Z"}
```

Use client_id and client_secret to get an access_token:

`curl -X POST -d "username=yourUsername&password=yourPassword&grant_type=password&client_id=yourClientID&client_secret=yourClientSecret" https://HOSTNAME/api/oauth2-ng/token/`

----------
Example call for creating a project with one RemoteContent Item(iframe)
```
from daemo import models, client
ti = models.RemoteContent('https://mywebsite.com/task/1', question_value='Iframe Title goes here', position=1)
t = models.Template(items=[ti])
p = models.Project(name='Project Name', price=0.1, repetition=1, template=t)
data = models.to_json(p)
d = client.DaemoClient(host='https://HOSTNAME/', access_token='yourAccessToken')

response = d.create_project(data)

```