import json
from marshmallow import Schema, fields as f, ValidationError, validate


class DittoProtocolMessage(Schema):
    topic = f.String(required=True)
    headers = f.Dict(required=False)
    path = f.String(required=True)
    value = f.Dict(required=True)
    fields = f.String(required=False)
    extra = f.Dict(required=False)


class CredentialsSchema(Schema):
    username = f.String(required=True)
    password = f.String(required=True)


class DeviceSchema(Schema):
    threadId = f.String(required=True)
    kafka_topic = f.String(required=True)
    kafka_server = f.String(required=True)
    #kafka_groupid = f.String(required=True)
    ditto_message = f.Nested(DittoProtocolMessage(), required=True)
    amqp_routingkey = f.String(required=True)
    amqp_credentials = f.Nested(CredentialsSchema())
    state = f.String(
        required=True, validate=validate.OneOf(["inactive", "active"]))


def checkDeviceSchema(data):
    DeviceSchema().load(data)
