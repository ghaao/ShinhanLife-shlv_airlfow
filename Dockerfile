# 1. Start from the official Airflow image
FROM apache/airflow:2.11.0-python3.11

# 2. Set Oracle environment variables for all subsequent layers
ENV ORACLE_CLIENT_VERSION="19.18"
ENV ORACLE_HOME=/opt/oracle/instantclient_19_18
ENV LD_LIBRARY_PATH=${ORACLE_HOME}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

# 3. Switch to the root user for system-level installations
USER root

# 4. Install system dependencies for Oracle Client and compilation
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libaio1 \
        unzip \
        curl \
        unixodbc-dev \
        gcc \
        g++ \
        python3-dev \
        build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 5. Download, install, and configure the Oracle Instant Client
RUN mkdir -p ${ORACLE_HOME} && \
    curl -o instantclient-basic.zip https://download.oracle.com/otn_software/linux/instantclient/1918000/instantclient-basic-linux.x64-19.18.0.0.0dbru.zip && \
    unzip instantclient-basic.zip -d /opt/oracle/ && \
    rm instantclient-basic.zip && \
    echo ${ORACLE_HOME} > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig

# 6. Switch back to the airflow user for runtime security
USER airflow

# 7. Install Python packages as the 'airflow' user
RUN pip install --no-cache-dir \
    "apache-airflow-providers-oracle==3.12.0" \
    "apache-airflow-providers-microsoft-mssql==3.8.0" \
    "apache-airflow-providers-mysql==5.7.3" \
    "apache-airflow-providers-mongo==3.6.0" \
    "apache-airflow-providers-postgres==5.11.0" \
    "cx_Oracle==8.3.0" \
    "oracledb==2.2.0" \
    "sqlalchemy==1.4.53" \
    "airflow-code-editor==5.2.2"