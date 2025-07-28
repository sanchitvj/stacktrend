# Azure Functions Dockerfile (equivalent to AWS Lambda container)
FROM mcr.microsoft.com/azure-functions/python:4-python3.12

# Set working directory
WORKDIR /home/site/wwwroot

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire src directory to the container
COPY src/ ./src/

# Copy function app configuration
COPY src/stacktrend/functions/host.json ./
COPY src/stacktrend/functions/local.settings.json ./

# Copy the specific function
COPY src/stacktrend/functions/github_collector/ ./github_collector/

# Set Python path to include our src directory
ENV PYTHONPATH=/home/site/wwwroot/src:$PYTHONPATH

# Azure Functions runtime will automatically start the function host 