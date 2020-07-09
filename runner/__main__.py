from flask import Flask, jsonify

# App initialization.
rnr = Flask(__name__)


@rnr.route("/", methods=["GET"])
def hello():
    return jsonify({"hello": "world"})


if __name__ == '__main__':
    rnr.run(host='0.0.0.0', debug=True)  # TODO: remove debug mode
