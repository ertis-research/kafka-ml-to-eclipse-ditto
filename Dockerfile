FROM python:3.9-slim-buster

ENV FLASK_APP="src/app"

WORKDIR /usr/src/app

COPY . .

RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 5000

#CMD ["python3", "-m", "flask", "run", "--host=0.0.0.0"]
CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]