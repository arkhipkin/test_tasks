# create a layer from python:3.7.4
FROM python:3.7.4-slim

# update apt-get
RUN apt-get update -qq && apt-get install -y apt-utils
RUN apt-get -y install gcc

# install requirments
# RUN python -m pip install --upgrade pip
RUN pip install --upgrade pip

# copy everything into root directory
COPY . /

# specify work directory as root
WORKDIR /

# ENTRYPOINT ["/usr/bin/python3"]
ENTRYPOINT ["python"] ["Anton_Arkhipkin.py"]

# To test:
# docker build . -t docker_python_n26
# docker run docker_python_n26 "Anton_Arkhipkin.py"