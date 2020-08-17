FROM python:3.8.3-slim

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY ./requirements.txt /usr/src/app/requirements.txt

# install selenium
RUN pip install -r requirements.txt
COPY ./requirements.txt /usr/src/app/requirements.txt
COPY . /usr/src/app
