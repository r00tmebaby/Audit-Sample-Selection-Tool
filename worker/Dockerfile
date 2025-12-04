FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY ../requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src ./src/

# Copy setup files
COPY setup.py pyproject.toml ./

# Install the package
RUN pip install --no-cache-dir -e .

# Create volume mount points
VOLUME ["/data/input", "/data/output"]

# Set entrypoint
ENTRYPOINT ["python", "-m", "main"]

# Default command shows help
CMD ["--help"]
