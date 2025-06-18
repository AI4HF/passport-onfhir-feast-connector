FROM python:3.10-slim

WORKDIR /app
COPY main.py /app/
COPY feast_models.py /app/
COPY passport_models.py /app/
COPY requirements.txt /app/
COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh
# Install python dependecies
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["./entrypoint.sh"]
