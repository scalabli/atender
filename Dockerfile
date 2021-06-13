FROM python:3.8

WORKDIR /root

COPY . /tmp/rq

RUN pip install /tmp/atender

RUN rm -r /tmp/atender

ENTRYPOINT ["atender", "worker"]
