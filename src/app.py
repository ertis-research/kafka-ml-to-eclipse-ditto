from flask import Flask
from connections import connectWithMongoDB
from threads import close_running_threads, initThreads
import atexit
import threading


# We start the Flask application
app = Flask(__name__)
# MongoDB connection
db, col = connectWithMongoDB(app)
# Register the function to be called on exit
atexit.register(close_running_threads)
# We start the threads that should be active
initThreads(db)
# Import API routes
import routes