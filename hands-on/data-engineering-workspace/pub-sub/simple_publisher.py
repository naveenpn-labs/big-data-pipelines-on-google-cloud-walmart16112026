from google.cloud.pubsub_v1 import PublisherClient
if __name__ == '__main__':
  publisher = PublisherClient()
  topic_name = "projects/sukhada-cb324/topics/my-demo-topic"
  data = '{"name":"Naveen Pn"}'
  message = bytes(data,'utf-8')
  future = publisher.publish(topic_name,message)
  print(future.result())

