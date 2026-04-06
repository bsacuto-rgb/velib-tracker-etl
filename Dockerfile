FROM python:3.11-slim

WORKDIR /velib_project

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_velib.py .

CMD ["python", "etl_velib.py"]