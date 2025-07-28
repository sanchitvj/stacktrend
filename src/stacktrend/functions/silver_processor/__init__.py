"""
Silver Data Processor Azure Function
Transforms raw GitHub data from bronze layer into clean, normalized silver layer data.
Updated: 2025-01-28 - Fix ingress to external
"""

import datetime
import logging
import azure.functions as func
import sys
import os

sys.path.insert(0, '/home/site/wwwroot/src')

try:
    from stacktrend.utils.azure_client import AzureStorageClient
    from stacktrend.config.settings import settings
    from stacktrend.utils.data_transformer import SilverTransformer
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
    from stacktrend.utils.azure_client import AzureStorageClient
    from stacktrend.config.settings import settings
    from stacktrend.utils.data_transformer import SilverTransformer


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Main function for silver data processing."""
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logging.info('HTTP trigger received for silver processing')
    logging.info(f'Silver processor function started via HTTP at {utc_timestamp}')
    
    try:
        # Validate configuration
        settings.validate()
        logging.info('Configuration validated successfully')
        
        # Initialize clients
        azure_client = AzureStorageClient()
        transformer = SilverTransformer()
        
        # Get recent bronze files to process
        logging.info('Finding bronze files to process...')
        bronze_files = azure_client.list_recent_bronze_files(hours_back=24)
        
        if not bronze_files:
            logging.info('No new bronze files found to process')
            return
            
        logging.info(f'Found {len(bronze_files)} bronze files to process')
        
        # Process each bronze file
        all_transformed_data = []
        processed_files = []
        
        for bronze_file in bronze_files:
            logging.info(f'Processing bronze file: {bronze_file}')
            
            # Download bronze data
            bronze_data = azure_client.download_json_data(
                container_name=settings.BRONZE_CONTAINER,
                blob_name=bronze_file
            )
            
            if not bronze_data:
                logging.error(f'Failed to download bronze file: {bronze_file}')
                continue
            
            # Debug: Log bronze data structure
            repositories = bronze_data.get('repositories', [])
            logging.info(f'Bronze file {bronze_file} contains {len(repositories)} repositories')
            if repositories:
                first_repo = repositories[0]
                logging.info(f'First repository type: {type(first_repo)}, sample: {str(first_repo)[:200]}...')
                
            # Transform the data
            transformed_repos = transformer.transform_repositories(
                repositories,
                bronze_data.get('metadata', {})
            )
            
            all_transformed_data.extend(transformed_repos)
            processed_files.append(bronze_file)
            
        if all_transformed_data:
            # Create silver layer data structure
            silver_data = {
                'metadata': {
                    'processing_timestamp': utc_timestamp,
                    'source_files': processed_files,
                    'total_repositories': len(all_transformed_data),
                    'transformation_version': '1.0'
                },
                'repositories': all_transformed_data
            }
            
            # Store in silver layer
            silver_blob_name = azure_client.generate_blob_name('processed_repositories')
            
            success = azure_client.upload_json_data(
                container_name=settings.SILVER_CONTAINER,
                blob_name=silver_blob_name,
                data=silver_data
            )
            
            if success:
                logging.info(f'Successfully stored transformed data in silver layer: {silver_blob_name}')
                logging.info(f'Processed {len(all_transformed_data)} repositories from {len(processed_files)} bronze files')
            else:
                logging.error('Failed to store data in silver layer')
        
        logging.info('Silver processing completed successfully')
        
        # Return success response for HTTP trigger
        return func.HttpResponse(
            f"Silver processing completed successfully at {utc_timestamp}",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f'Error in silver processor: {str(e)}', exc_info=True)
        
        # Return error response for HTTP trigger
        return func.HttpResponse(
            f"Error in silver processor: {str(e)}",
            status_code=500
        )
