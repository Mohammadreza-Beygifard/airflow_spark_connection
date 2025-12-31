# syntax=docker/dockerfile:1.6
FROM --platform=linux/arm64 ubuntu:22.04

ARG SPARK_VERSION=3.5.0
ARG HADOOP_PROFILE=hadoop3
ARG SPARK_ARCHIVE=spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}
ARG SPARK_TGZ=${SPARK_ARCHIVE}.tgz

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Core deps + Java 11
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl wget bash tini \
    openjdk-11-jdk-headless \
    build-essential \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Python 3.11 (Ubuntu 22.04 needs deadsnakes PPA for 3.11)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gnupg gpg-agent dirmngr \
    && rm -rf /var/lib/apt/lists/*

RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && apt-get install -y --no-install-recommends \
    python3.11 python3.11-venv python3.11-distutils python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Make python3 point to python3.11
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    python3 -m pip install --no-cache-dir --upgrade pip

# Install Spark 3.5.0 (official Apache distro)
RUN mkdir -p /opt && \
    curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}" -o "/tmp/${SPARK_TGZ}" && \
    tar -xzf "/tmp/${SPARK_TGZ}" -C /opt && \
    ln -s "/opt/${SPARK_ARCHIVE}" "${SPARK_HOME}" && \
    rm -f "/tmp/${SPARK_TGZ}"

# Optional: set PySpark python to 3.11
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
