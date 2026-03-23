from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "Bot is perfectly alive and running!"

def run():
    # Render assigns a specific port, default to 8080
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()