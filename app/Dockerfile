FROM ubuntu:18.04
RUN apt update && apt install python3 python3-pip -y
ENV LANG C.UTF-8
WORKDIR /app
ADD requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY app.py app.py
CMD ["python3", "-u", "app.py"]