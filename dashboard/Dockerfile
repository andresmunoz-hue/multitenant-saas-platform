# Use Temurin base (often already cached) to avoid pulling python:slim when registry DNS fails.
FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY dashboard/requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY dashboard/app.py /app/app.py

ENV DASHBOARD_DATA_ROOT=/data
EXPOSE 8501

CMD ["python3", "-m", "streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--browser.gatherUsageStats=false"]
