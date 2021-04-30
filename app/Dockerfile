FROM python:3.7-slim
WORKDIR /app
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY app.py app.py
CMD ["python", "-u", "app.py"]