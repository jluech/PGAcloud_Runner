import logging

from flask import Flask, jsonify

logging.basicConfig(level=logging.DEBUG)  # TODO: remove and reduce to INFO


# App initialization.
rnr = Flask(__name__)


@rnr.route("/status", methods=["GET"])
def status():
    return "OK"


@rnr.route("/<int:pga_id>", methods=["GET"])
def hello(pga_id):
    logging.debug("RECEIVED CALL FOR {}".format(pga_id))
    return jsonify({"hello": "world"})


if __name__ == '__main__':
    rnr.run(host='0.0.0.0', debug=True)  # TODO: remove debug mode
