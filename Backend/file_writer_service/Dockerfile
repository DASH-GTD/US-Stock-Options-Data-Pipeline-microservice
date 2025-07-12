FROM python:3.9-slim

WORKDIR /app 
# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    apt-transport-https \
    gnupg \
    unixodbc-dev \
    gcc \
    g++ \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server (for Azure SQL)
# RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
#     curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
#     apt-get update && \
#     ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
#     rm -rf /var/lib/apt/lists/*
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt 
COPY . .
EXPOSE 8003

CMD ["gunicorn", "--bind", "0.0.0.0:8003", "file_writer_service.wsgi:application"]

