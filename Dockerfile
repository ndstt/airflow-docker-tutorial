FROM apache/airflow:2.10.3-python3.10

# Copy requirements file
COPY requirements.txt /requirements.txt

# Update pip and install necessary dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir setuptools==65.5.0 wheel  # Install setuptools and wheel explicitly

RUN pip install --no-cache-dir numpy==1.26.0

# Install remaining requirements
RUN pip install --no-cache-dir -r /requirements.txt