import boto3
import json
import os
import logging
import time
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS Configuration
AwsAccountId = os.environ.get('AWS_ACCOUNT_ID', '1234567890987')
AwsRegion = os.environ.get('AWS_REGION', 'us-east-1')
UniqueId = 'MigratedDEV'

def import_quicksight_bundle():
    try:
        # Initialize QuickSight client
        quicksight = boto3.client('quicksight', region_name=AwsRegion)
        
        # Read the asset bundle file
        try:
            with open("./src/QuickSightAssetBundle-Modified.zip", "rb") as qsFile:
                qsFileContents = qsFile.read()
        except FileNotFoundError:
            logger.error("Asset bundle file not found")
            return False
        except Exception as e:
            logger.error(f"Error reading asset bundle file: {e}")
            return False

        # Start the import job
        logger.info(f"Starting import job: AAB-{UniqueId}")

        try:
            response = quicksight.start_asset_bundle_import_job(
                AwsAccountId=AwsAccountId,
                AssetBundleImportJobId=f'AAB-{UniqueId}',

                AssetBundleImportSource={
                    'Body': qsFileContents
                }
            )
            logger.info(f"Import job started: {response}")
        except ClientError as e:
            logger.error(f"Error starting import job: {e}")
            return False

        # Monitor the import job status
        status = 'QUEUED_FOR_IMMEDIATE_EXECUTION'
        start_time = time.time()
        timeout = 900  # 5 minutes timeout

        while status in ['QUEUED_FOR_IMMEDIATE_EXECUTION', 'IN_PROGRESS']:
            if time.time() - start_time > timeout:
                logger.error("Import job timed out")
                return False

            try:
                response = quicksight.describe_asset_bundle_import_job(
                    AwsAccountId=AwsAccountId,
                    AssetBundleImportJobId=f'AAB-{UniqueId}'
                )
                status = response['JobStatus']
                logger.info(f"Current status: {status}")
                
                if status == 'SUCCESSFUL':
                    logger.info("Import job completed successfully")
                    return True
                elif status =='FAILED_ROLLBACK_IN_PROGRESS'  :
                    for error in response['Errors']:
                        logger.error(f"Import job failed and rollback in progress : {error}.")
                elif status == 'FAILED':
                    logger.error(f"Import job failed: {response['Errors']}")
                    return False
                
                time.sleep(5)

            except ClientError as e:
                logger.error(f"Error checking job status: {e}")
                return False

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def main():
    logger.info("Starting QuickSight asset bundle import process")
    
    if import_quicksight_bundle():
        logger.info("Asset bundle import process completed successfully")
    else:
        logger.error("Asset bundle import process failed")

if __name__ == "__main__":
    main()
