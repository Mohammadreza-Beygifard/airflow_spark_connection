FROM apache/airflow:2.9.3-python3.10

# Install Spark provider with constraints
RUN pip install --no-cache-dir "apache-airflow-providers-apache-spark<5.0.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"

# Install Java (OpenJDK 17) and ps command procps is needed to enable spark using ps to find java, 
# and iputils-ping netcat are needed to enable the developer to check the connections of the airflow container
USER root
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    procps \
    iputils-ping \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

EXPOSE  8080

# Switch back to airflow user
USER airflow