# # 1. Use a lightweight Python base image
# FROM python:3.11-slim

# # 2. Set the working directory inside the container
# WORKDIR /app

# # 3. Copy requirements and install dependencies
# # We do this first so Docker caches the installed packages
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # 4. Copy your application files
# # COPY app.py .
# # COPY pipeline.py .
# # COPY embeddings_V2.py .
# # COPY vector_db_V2.py .

# COPY . .

# # 5. Define the start command
# # "app:app" means: look in file 'app.py' for the object named 'app'
# # We use the $PORT environment variable which Render automatically provides
# CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]

# 1. Use Python 3.11 Slim (Debian-based)
FROM python:3.11-slim

# 2. Set working directory
WORKDIR /app

# 3. Install system dependencies (curl is needed to download Qdrant)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# 4. Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Download Qdrant Binary (Linux x86_64 for Render)
# We download version 1.12.1 (stable), extract it, and make it executable.
RUN curl -L https://github.com/qdrant/qdrant/releases/download/v1.12.1/qdrant-x86_64-unknown-linux-gnu.tar.gz \
    | tar xz -C /app && chmod +x /app/qdrant

# 6. Copy your application code
COPY . .

# 7. Define the Startup Command
# This script does 3 things:
#   1. Starts Qdrant in the background (&) on default port 6333
#   2. Sleeps for 5 seconds to give Qdrant time to initialize
#   3. Starts your FastAPI app on the port Render provides
CMD ["sh", "-c", "./qdrant > /dev/null 2>&1 & sleep 5 && uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]