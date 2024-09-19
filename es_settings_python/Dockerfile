# https://hub.docker.com/_/python
FROM python
ENV PATH /usr/src/app/bin:$PATH
ENV HF_HOME /tmp

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

#COPY bin/* ./
#COPY python-manager.sh /
#ENTRYPOINT [ "/python-manager.sh" ]
ENTRYPOINT ["python", "./bin/es_settings.py"]

