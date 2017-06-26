FROM python:2.7

RUN mkdir /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY gather.py .

CMD [ "python", "-u","./gather.py" ]
