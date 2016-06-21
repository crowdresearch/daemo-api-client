from daemo.client import Client
from daemo.models import Project


if __name__=="__main__":
    client = Client(access_token='hnstvlyettlnsuvdtir')
    client.publish()


# from daemo import models, client
# ti = models.RemoteContent('https://mywebsite.com/task/1', question_value='Iframe Title goes here', position=1)
# t = models.Template(items=[ti])
# p = models.Project(name='Project Name', price=0.1, repetition=1, template=t)
# data = models.to_json(p)
# d = client.DaemoClient(host='https://HOSTNAME/', access_token='yourAccessToken')
#
# response = d.create_project(data)
