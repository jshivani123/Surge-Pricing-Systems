
from kafka import KafkaProducer
import json
from time import sleep

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

# reading file
file_data = open("Course4Dataset-data.csv","r")
for line in file_data:
  words = line.replace("\n","").split(",")
  data = json.dumps({"id":words[0], "lat":words[1], "long":words[2], "ts":words[                                                                             3], "type":words[4]}).encode('utf-8')
  ack = producer.send("surgePriceDemo",value = data)
  metadata = ack.get()
  print(metadata.topic,metadata.partition)
  sleep(1)
