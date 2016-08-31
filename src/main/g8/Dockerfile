FROM python:2.7
COPY requirements.txt /usr/src/app/requirements.txt
RUN pip install --no-cache-dir  --trusted-host $pypi_repo$ -r /usr/src/app/requirements.txt
ADD . /app
RUN cd /app && python -m unittest discover
ENTRYPOINT ["python2"]
