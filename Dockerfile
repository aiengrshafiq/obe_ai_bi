# Use python 3.10-slim
FROM python:3.10-slim

WORKDIR /app

# 1. Install Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the entire application
COPY . .

# --- 3. BUILD-TIME INTELLIGENCE INJECTION ---
# We define build arguments (Secrets passed during docker build)
ARG DASHSCOPE_API_KEY
ARG AI_MODEL_NAME
ARG AI_BASE_URL

# Set them as ENV variables temporarily so the script can see them
ENV DASHSCOPE_API_KEY=$DASHSCOPE_API_KEY
ENV AI_MODEL_NAME=$AI_MODEL_NAME
ENV AI_BASE_URL=$AI_BASE_URL

# RUN the training script. This creates the 'vanna_storage' folder inside the image.
RUN python -m app.services.build_vanna

# Clean up secrets (Optional security measure, though env vars persist in history)
# Note: For strict security, use Docker --secret mounts, but ARGs are standard for this level.
# --------------------------------------------

# 4. Expose the port
EXPOSE 8000

# 5. Start Command
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]