# Use python 3.10 as discussed
FROM python:3.10-slim

WORKDIR /app

# 1. Install Dependencies
# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the entire 'app' directory
COPY . .

# 3. Expose the port for SAE
EXPOSE 8000

# 4. START COMMAND (Critical Change)
# Old: "main:app"
# New: "app.main:app" (Because main.py is inside the 'app' folder)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]