from flask import Flask,request

app = Flask(__name__)

@app.route("/")
def index():
    return "Server is running..."

@app.route("/health-check")
def check():
    return "Checking..."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port = '5000')

