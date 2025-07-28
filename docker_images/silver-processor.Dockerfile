FROM mcr.microsoft.com/azure-functions/python:4-python3.12

# Set the working directory
WORKDIR /home/site/wwwroot

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Copy function-specific files
COPY src/stacktrend/functions/host.json ./
COPY src/stacktrend/functions/local.settings.json ./
COPY src/stacktrend/functions/silver_processor/ ./silver_processor/

# Set Python path
ENV PYTHONPATH=/home/site/wwwroot/src:$PYTHONPATH

# Set Azure Functions environment variables
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true 