# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.11.2-slim-bullseye

# adding custom MS repository
RUN apt-get update
RUN apt-get install curl gnupg2 -y
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# install SQL Server drivers
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
RUN /bin/bash -c "source ~/.bashrc"
# optional: for unixODBC development headers
RUN apt-get install -y unixodbc unixodbc-dev

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .

# RUN pip install pyodbc --no-binary :all: pyodbc
RUN apt-get update
# build-essential is required to insure pyodbc compile
RUN apt-get install --reinstall build-essential -y

RUN pip install pyodbc openpyxl
RUN python -m pip install -r requirements.txt

WORKDIR /app
COPY . /app

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

EXPOSE 5500

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["gunicorn", "-w", "2" , "-b", "0.0.0.0:5500", "--chdir", "/app", "app:app"]
