FROM spark-worker:3.0.0
RUN apt-get update && apt-get upgrade -y
RUN apt-get install python3-pip -y
RUN pip3 install shapely