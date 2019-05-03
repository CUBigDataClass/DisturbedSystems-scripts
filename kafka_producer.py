from flask import Flask
from kafka import KafkaProducer
from flask_restful import Resource, Api, reqparse

# SCRIPT PARAMETERS
API_HOST = '0.0.0.0'
API_PORT = 5600
KAFKA_TOPIC = 'test'
KAFKA_CLUSTER = ['10.142.0.59:9092', '10.142.0.60:9092', '10.142.0.61:9092']


# API RESOURCE
class KafkaProducerApi(Resource):

    def __init__(self, kafka_producer):
        self.producer = kafka_producer

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("message")
        args = parser.parse_args()

        message = args["message"]
        self.producer.send(KAFKA_TOPIC, value=message)
        return message, 200


if __name__ == '__main__':
    kafka = KafkaProducer(bootstrap_servers=KAFKA_CLUSTER)

    app = Flask(__name__)
    api = Api(app)
    api.add_resource(KafkaProducerApi, "/logs", resource_class_args=(kafka,))

    app.run(host=API_HOST, port=API_PORT)
