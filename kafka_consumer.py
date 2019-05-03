from kafka import KafkaConsumer

k = KafkaConsumer(
    'test',
    bootstrap_servers=['10.142.0.59', '10.142.0.60', '10.142.0.61']
)

for message in k:
    print("Lo! A tweet!\t\t{}".format(message))

