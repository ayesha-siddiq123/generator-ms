FROM python:3.7

WORKDIR /python_app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "bash", "adapter/VSK_data_transformation.sh"]
