from flask import Flask
import json

app = Flask(__name__)

monitor: bool = False

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/false")
def false():
    return json.dumps(False)

@app.route("/true")
def true():
    return json.dumps(True)

@app.route("/change")
def change():
    global monitor
    monitor = not monitor
    return json.dumps(monitor)

@app.route("/monitor")
def monitor():
    global monitor
    return json.dumps(monitor)

app.run(port=5000)