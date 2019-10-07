FROM python:3

EXPOSE 9090
EXPOSE 9099

WORKDIR /usr/app

COPY . .

CMD [ "python", "./main.py" ]