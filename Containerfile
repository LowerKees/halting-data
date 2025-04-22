FROM python:3.11-slim

# Set environment variables
ENV SPARK_VERSION=3.5.5
ENV DELTA_VERSION=2.4.0
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Upgrade systemwide to setuptools v 70.0.0 or higher because of CVE-2024-6345 (remote code execution via setuptools download function)
RUN pip install setuptools>=70.0.0 

# Get latest version of pip
RUN pip install pip --upgrade 

WORKDIR /app

# Create virtual env to put the installation in and activate it
RUN python -m venv .venv && . ./.venv/bin/activate

# # Install PySpark and Delta Lake
# RUN pip install --no-cache-dir \
#     pyspark==${SPARK_VERSION} \
#     delta-spark==${DELTA_VERSION} \
#     pandas \
#     pyarrow

# # # Verify installation
# # RUN pyspark --version

# # # Command to run when container starts
# # CMD ["bash"]