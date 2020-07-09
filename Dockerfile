FROM python:3
MAINTAINER "Janik Luechinger janik.luechinger@uzh.ch"

COPY . /pga
WORKDIR /pga

RUN apt-get -y update && apt-get -y upgrade
RUN pip install -U pip && pip install -r requirements.txt

ENTRYPOINT [ "python", "-m", "runner" ]

# Manual image building
# docker build -t pga-cloud-runner .
# docker tag pga-cloud-manager jluech/pga-cloud-runner
# docker push jluech/pga-cloud-runner
