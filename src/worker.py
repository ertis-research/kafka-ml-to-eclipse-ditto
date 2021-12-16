from flask import jsonify
from marshmallow.exceptions import ValidationError
from connections import connectWithKafka, connectWithRabbitMQ
from schemas import checkDeviceSchema
from re import compile
from threading import Thread, currentThread, Event
from pika.exceptions import ChannelClosed, ConnectionClosed, StreamLostError
from kafka.errors import KafkaError
import json


def addValuesToOutputMessage(msg, values, pattern):
    for key in msg:
        value = msg[key]
        if isinstance(value, dict):
            addValuesToOutputMessage(value, values, pattern)
        elif isinstance(value, str) and pattern.match(value):
            num = int(value[1:-1])
            if(num < len(values)):
                msg[key] = values[int(value[1:-1])]

def publishMessage(channel, connection, device, output):
    channel.basic_publish(exchange='',
                        routing_key=device['amqp_routingkey'],
                        body=json.dumps(output)
                        )
    print("[Log " + device['threadId'] + "]: Message sent")
    

class Worker(Thread):

    def __init__(self, device):
        super().__init__()
        self.device = device
        self._stop_event = Event()

    def stop(self):
        self._stop_event.set()
        print("[Log " + self.device['threadId'] + "]: Stopped")

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        device = self.device

        # Compruebo que el objeto es correcto
        try:
            checkDeviceSchema(device)
        except ValidationError as err:
            print(err.messages)

        # Configuro el consumidor de Kafka
        consumer = connectWithKafka(device)

        # Configuro el broker de AMQP
        channel, connection = connectWithRabbitMQ(device)

        # Creo el pattern para asignar los valores
        pattern = compile("{[0-9]*}")
        output = {}

        print("[Log " + device['threadId'] + "]: Listening")
        while not self.stopped():
            try:
                msg = consumer.poll()
            except KafkaError:
                consumer = connectWithKafka(device)
                msg = None
            
            if not msg:
                continue
            else:
                for valuesDict in list(msg.values()):
                    for consumerRecord in valuesDict:
                        try:
                            msgjson = json.loads(consumerRecord.value.decode())
                            values = msgjson['values']
                            output = device['ditto_message']
                            value = output['value']
                            addValuesToOutputMessage(value, values, pattern)
                            output['value'] = value
                            publishMessage(channel, connection, device, output)
                        except (ChannelClosed, ConnectionClosed, StreamLostError):
                            channel, connection = connectWithRabbitMQ(device)
                            publishMessage(channel, connection, device, output)
                        except:
                            print("[Log " + device['threadId'] + "]: Error")
        consumer.close()
        if(channel.is_open):
            channel.close()
        if(connection.is_open):
            connection.close()
        print("[Log " + device['threadId'] + "]: Closed")
