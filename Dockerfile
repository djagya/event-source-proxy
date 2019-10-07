FROM python:3

WORKDIR /usr/app

COPY . .

CMD [ "python", "./main.py" ]