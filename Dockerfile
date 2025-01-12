#
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

#


WORKDIR /app



# Install curl and gnupg2
RUN apt-get update && \
    apt-get install -y curl gnupg2

# Add Microsoft GPG key
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-archive-keyring.gpg

# Add Microsoft SQL Server repository
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-archive-keyring.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list

# Update package list again after adding repository
RUN apt-get update

# Install ODBC driver for SQL Server
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc unixodbc-dev

#
COPY main.py models.py database.py consumer.py chart_properties.py utils.py requirements.txt /app/


#
RUN pip install --no-cache-dir -r requirements.txt

# Run the consumer
#CMD ["python", "/app/consumer.py"]

# #
#CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9000"]
