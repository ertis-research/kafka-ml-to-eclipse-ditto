from kafka import KafkaConsumer
from consts import RABBITMQ_ENDPOINT, RABBITMQ_PORT
from consts import MONGO_URI
from flask_pymongo import PyMongo
import pika


def connectWithMongoDB(app):
    app.config["MONGO_URI"] = MONGO_URI
    mongodb_client = PyMongo(app)
    db = mongodb_client.db
    col = db.devices
    print("[Log APP]: Connected to MongoDB")
    return db, col


def connectWithKafka(device):
    consumer = KafkaConsumer(device['kafka_topic'],
                            api_version=(0, 10, 1),
                            bootstrap_servers=device['kafka_server'],
                            # group_id=device['kafka_groupid'],
                            auto_offset_reset='latest')
    print("[Log " + device['threadId'] + "]: Connected to KafkaML")
    return consumer


def connectWithRabbitMQ(device):
    if('amqp_credentials' in device):
        cred = device['amqp_credentials']
        credentials = pika.PlainCredentials(
            cred['username'], cred['password'])
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_ENDPOINT, port=RABBITMQ_PORT, credentials=credentials))
    else:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_ENDPOINT, port=RABBITMQ_PORT))
    channel = connection.channel()
    channel.queue_declare(queue=device['amqp_routingkey'])
    print("[Log " + device['threadId'] + "]: Connected to RabbitMQ")
    return channel, connection
