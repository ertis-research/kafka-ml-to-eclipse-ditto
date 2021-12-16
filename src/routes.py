from marshmallow.exceptions import ValidationError
from flask import request, jsonify, Response
from schemas import checkDeviceSchema
from bson.json_util import dumps
from threads import createThread, destroyThread
from consts import FIELD_ID
from app import app, col # Get flask app and mongodb collection

def query(id):
    """Auxiliary function to abstract the query that identifies the devices"""
    return {FIELD_ID: id}


@app.route("/")
def root():
    return "<p>Api docs</p>"


@app.route("/devices", methods=['POST', 'GET'])
def devices():
    if request.method == 'POST':
        # Get the data
        data = request.json

        try:
            # Check that it complies with the scheme
            checkDeviceSchema(data)
        except ValidationError as err:
            return jsonify(err.messages), 400

        # Save the data to the database
        col.insert_one(data)

        msg = ""
        if(data['state'] == 'active'):
            # If it is active we start the thread
            createThread(data)
            msg = "Thread started."

        return jsonify(message=("Device created successfully." + msg))

    else:
        # Get all devices
        devices = col.find()

        # Return a list with all devices
        return Response(
            dumps([device for device in devices]),
            mimetype='application/json'
        )


@app.route('/devices/<id>', methods=['GET', 'DELETE'])
def devices_id(id):
    if request.method == 'DELETE':
        # Look for the device with given id
        device = col.find_one(query(id))

        # Close and remove the thread for that device
        msg = ""
        if(device != None and device['state'] == 'active'):
            destroyThread(id)
            msg = "Thread closed."

        # Try to remove the device with given id
        result = col.delete_one(query(id))

        # Returns the result and delete his thread if it was active
        if (result.deleted_count > 0):
            return jsonify(message=("Device with id " + id + " removed successfully" + msg))
        else:
            return jsonify(message=("There is no device with id " + id))

    else:
        # Look for the device with given id
        device = col.find_one(query(id))

        # Returns the result
        if (device != None):
            return Response(
                dumps(device),
                mimetype='application/json'
            )
        else:
            return jsonify(message=("There is no device with id " + id))


@app.route('/devices/<id>/start', methods=['PUT'])
def start_device(id):
    if request.method == 'PUT':
        # Look for the device with given id
        device = col.find_one(query(id))

        # If the device exists and is not active, we start it
        if device == None:
            return jsonify(message=("There is no device with id " + id))

        elif (device['state'] == 'inactive'):
            col.update_one(query(id), {"$set": {"state": "active"}})
            createThread(device)
            return jsonify(message="Thread for device with id " + id + " started successfully")

        else:
            return jsonify(message="Thread for the device with id " + id + " was already started") 


@app.route('/devices/<id>/stop', methods=['PUT'])
def stop_device(id):
    if request.method == 'PUT':
        # Look for the device with given id
        device = col.find_one(query(id))

        # If the device exists and is active, we stopped it
        if device == None:
            return jsonify(message=("There is no device with id " + id))

        if(device['state'] == 'active'):
            col.update_one(query(id), {"$set": {"state": "inactive"}})
            destroyThread(id)
            return jsonify(message="Thread for device with id " + id + " stopped successfully")
        
        else: 
            return jsonify(message="Thread for the device with id " + id + " was already stopped")