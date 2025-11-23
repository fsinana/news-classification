# Use slim Python base image
FROM python:3.11-slim

# --- System dependencies for PySpark (needs Java) ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk-headless \
 && rm -rf /var/lib/apt/lists/*

# Java env vars so PySpark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Set workdir
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code into image
COPY . .

# Streamlit port
EXPOSE 8501

# Default command: run dashboard
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
