#!/usr/bin/env bash
set -e

# Initialize the Airflow DB
airflow db init

# Check if admin user exists
USER_EXISTS=$(airflow users list | grep -c admin || true)

if [ "$USER_EXISTS" -eq 0 ]; then
    airflow users create \
      --username admin \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@example.com \
      --password admin
else
    echo "Admin user already exists, skipping creation"
fi

airflow connections add spark_conn \
    --conn-type spark_connect \
    --conn-host spark://spark-master \
    --conn-port 7077 \
    --conn-extra '{"use_ssl": false}'

