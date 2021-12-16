from consts import FIELD_ID
from worker import Worker

threads = {}


def createThread(device):
    device.pop('_id')
    t = Worker(device)
    t.daemon = True
    threads[device[FIELD_ID]] = t
    t.start()


def destroyThread(id):
    if id in threads:
        t = threads[id]
        t.stop()
        threads.pop(id)


def initThreads(database):
    global threads
    threads = {}
    devices = database.devices.find()
    for device in devices:
        if('state' in device and device['state'] == "active"):
            createThread(device)


def close_running_threads():
    listThreads = list(threads.values())
    for t in listThreads:
        t.stop()
        t.join()
    print("Exiting correctly")
