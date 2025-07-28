"""
GitHub Data Collector Azure Function
Runs every 6 hours to collect trending repository data and store in Azure Storage bronze layer.
Updated: 2025-01-28 - Force redeployment to fix blob naming
"""

import datetime
import logging
import azure.functions as func
import sys
import os

sys.path.insert(0, '/home/site/wwwroot/src')

try:
    from stacktrend.utils.github_client import GitHubClient
    from stacktrend.utils.azure_client import AzureStorageClient
    from stacktrend.config.settings import settings
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
    from stacktrend.utils.github_client import GitHubClient
    from stacktrend.utils.azure_client import AzureStorageClient
    from stacktrend.config.settings import settings


def main(mytimer: func.TimerRequest = None, req: func.HttpRequest = None):
    """Main function for GitHub data collection."""
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    # Handle HTTP trigger for testing
    if req is not None:
        logging.info('🧪 HTTP trigger received for manual testing')
        trigger_type = "HTTP"
    else:
        if mytimer and mytimer.past_due:
            logging.info('The timer is past due!')
        trigger_type = "Timer"

    logging.info(f'GitHub collector function triggered via {trigger_type} at %s', utc_timestamp)
    
    try:
        # Validate configuration
        settings.validate()
        logging.info('✅ Configuration validated successfully')
        
        # Initialize clients
        github_client = GitHubClient()
        azure_client = AzureStorageClient()
        
        # Check GitHub rate limits
        rate_limit_info = github_client.get_rate_limit_info()
        logging.info(f'GitHub rate limit: {rate_limit_info.get("core", {}).get("remaining", "unknown")} remaining')
        
        # Collect trending repositories
        logging.info('🔄 Starting GitHub data collection...')
        
        # Collect repositories for different languages
        languages = ['python', 'javascript', 'typescript', 'java', 'go', 'rust', 'cpp']
        all_repos = []
        
        for language in languages:
            logging.info(f'Collecting {language} repositories...')
            repos = github_client.get_trending_repositories(language=language, limit=20)
            
            # Add language tag to each repo
            for repo in repos:
                repo['collection_language'] = language
                repo['collection_timestamp'] = utc_timestamp
            
            all_repos.extend(repos)
            logging.info(f'✅ Collected {len(repos)} {language} repositories')
        
        # Also collect general trending (no language filter)
        logging.info('Collecting general trending repositories...')
        general_repos = github_client.get_trending_repositories(language=None, limit=50)
        for repo in general_repos:
            repo['collection_language'] = 'general'
            repo['collection_timestamp'] = utc_timestamp
        all_repos.extend(general_repos)
        
        logging.info(f'✅ Total repositories collected: {len(all_repos)}')
        
        # Store data in Azure Storage bronze layer
        if all_repos:
            blob_name = azure_client.generate_blob_name('github_repositories')
            
            collection_data = {
                'metadata': {
                    'collection_timestamp': utc_timestamp,
                    'total_repositories': len(all_repos),
                    'languages_collected': languages + ['general'],
                    'rate_limit_remaining': rate_limit_info.get('core', {}).get('remaining', 'unknown')
                },
                'repositories': all_repos
            }
            
            success = azure_client.upload_json_data(
                container_name=settings.BRONZE_CONTAINER,
                blob_name=blob_name,
                data=collection_data
            )
            
            if success:
                logging.info(f'✅ Successfully stored data in bronze layer: {blob_name}')
            else:
                logging.error('❌ Failed to store data in Azure Storage')
        
        # Final rate limit check
        final_rate_limit = github_client.get_rate_limit_info()
        logging.info(f'Final GitHub rate limit: {final_rate_limit.get("core", {}).get("remaining", "unknown")} remaining')
        
        logging.info('✅ GitHub collection completed successfully')
        
        # Return success response for HTTP trigger
        if req is not None:
            return func.HttpResponse(
                f"✅ GitHub collection completed successfully at {utc_timestamp}",
                status_code=200
            )
        
        # For timer triggers, just return None
        return None
        
    except Exception as e:
        logging.error(f'❌ Error in GitHub collector: {str(e)}', exc_info=True)
        
        # Return error response for HTTP trigger
        if req is not None:
            return func.HttpResponse(
                f"❌ Error in GitHub collector: {str(e)}",
                status_code=500
            )
        raise 