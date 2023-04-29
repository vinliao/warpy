# Use an official Python runtime as a parent image
FROM python:3.11-slim-buster

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the cacert.pem file to the container
COPY ./src/db/cacert.pem /etc/ssl/cert.pem

# Expose the port specified in the PORT environment variable
ENV PORT=8000
EXPOSE $PORT

# Run the app using Uvicorn
CMD python main.py