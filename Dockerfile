FROM python:3.7

WORKDIR /python_flask

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

COPY config.ini transformer_generator/transformers/ 
CMD [ "python3", "transformer_generator/transformer_generator.py"]


