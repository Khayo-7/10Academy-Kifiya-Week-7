# Use official Python base image
FROM python:3.10

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r requirements.txt

# Expose FastAPI port
EXPOSE 7777

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7777"]
