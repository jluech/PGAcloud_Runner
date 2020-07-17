import logging

from flask import Flask, request

logging.basicConfig(level=logging.DEBUG)  # TODO: remove and reduce to INFO


# App initialization.
rnr = Flask(__name__)


@rnr.route("/status", methods=["GET"])
def status():
    return "OK"


@rnr.route("/population", methods=["POST"])
def init_population():
    use_population = request.args.get("use_population")
    logging.debug("Initializing population: {init_}".format(init_=not use_population))


if __name__ == '__main__':
    rnr.run(host='0.0.0.0', debug=True)  # TODO: remove debug mode
