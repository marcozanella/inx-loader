# Download base image ubuntu 22.04
FROM ubuntu:22.04

ENV DIRPATH=/loader_app

# Label the image
LABEL maintainer="marco.zanella@inxeurope.com"
LABEL version=1.0
LABEL description="inx-loader web app based on Flask"

# setting base directory
WORKDIR $DIRPATH

# Disable interactive prompts
ARG DEBIAN_FRONTEND=noninteractive

# Update Ubuntu Software repository
RUN apt-get update
RUN apt-get install curl gnupg2 -y

# apt-get and system utilities
RUN apt-get update && apt-get install -y \
    curl apt-utils apt-transport-https debconf-utils gcc build-essential \
    && rm -rf /var/lib/apt/lists/*
# adding custom MS repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# install SQL Server drivers
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
RUN /bin/bash -c "source ~/.bashrc"
# optional: for unixODBC development headers
RUN apt install -y unixodbc-dev

# python libraries
RUN apt-get update && apt-get install -y python3 python3-pip libpq-dev python3-dev

RUN pip install pyodbc
RUN pip install Flask

# copy the requirements file into the image
COPY ./requirements.txt $DIRPATH/requirements.txt

RUN cd $DIRPATH
RUN pip install -r ./requirements.txt

# # Expose Port for the Application 
# EXPOSE 5000