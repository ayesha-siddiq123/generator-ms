FROM python:3.7

WORKDIR /python_flask

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

COPY config.ini generators/transformers/python_files 
CMD [ "python3", "generators/generator.py"]
