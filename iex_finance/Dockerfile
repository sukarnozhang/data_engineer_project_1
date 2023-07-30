FROM python:3.9.7-slim

WORKDIR /src

COPY /src .

RUN pip install -r requirements.txt 

ENV PYTHONPATH=/src
CMD ["python", "pipeline/pipeline.py"]
