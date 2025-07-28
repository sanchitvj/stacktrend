# Azure Functions Dockerfile for Container Apps
FROM mcr.microsoft.com/azure-functions/python:4-python3.12

# Set working directory
WORKDIR /home/site/wwwroot

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire src directory to the container (our utilities)
COPY src/ ./src/

# Copy function app configuration from src/stacktrend/functions/ to container root
COPY src/stacktrend/functions/host.json ./
COPY src/stacktrend/functions/local.settings.json ./

# Copy the specific function from src/stacktrend/functions/github_collector/ to container root
COPY src/stacktrend/functions/github_collector/ ./github_collector/

# Set Python path to include our src directory
ENV PYTHONPATH=/home/site/wwwroot/src:$PYTHONPATH

# Set Azure Functions specific environment variables
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# Azure Functions runtime will automatically start the function host 