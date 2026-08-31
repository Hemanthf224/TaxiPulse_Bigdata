FROM apache/spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

# Install system dependencies, curl, openjdk-11, python3-pip, sbt
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    procps \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

# Install Python libraries
RUN pip3 install --no-cache-dir \
    "pandas>=2.0.0,<2.1.0" \
    "pyarrow>=14.0.0,<15.0.0" \
    requests \
    streamlit \
    plotly \
    folium \
    kafka-python

# Setup Hadoop configuration directory
RUN mkdir -p /etc/hadoop/conf
RUN echo '<?xml version="1.0"?><configuration><property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property></configuration>' > /etc/hadoop/conf/core-site.xml

ENV HADOOP_CONF_DIR=/etc/hadoop/conf

WORKDIR /workspace
