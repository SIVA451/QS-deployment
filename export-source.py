import boto3
import json
import os
import urllib3
import zipfile
import sys
import time
import logging
import shutil
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
AWS_ACCOUNT_ID = '1234567890'
AWS_REGION = 'us-east-1'
PARENT_FOLDER_ID = FOLDER_ID = 's221172e-ea74-481a-87e9-d37c5676ec6b'


TEMP_DIR = './DevState'
TEMP_ZIP = 'QuickSightAssetBundle.zip'
OUTPUT_ZIP = './src/QuickSightAssetBundle-Modified.zip'


def cleanup_temp_files():
    """Clean up temporary files and directories"""
    try:
        if os.path.exists(TEMP_DIR):
            shutil.rmtree(TEMP_DIR)
        if os.path.exists(TEMP_ZIP):
            os.remove(TEMP_ZIP)
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

def start_export_job():
    """Start and monitor the QuickSight asset bundle export job"""
    try:
        quicksight = boto3.client('quicksight')
        
        # Start export job
        response = quicksight.start_asset_bundle_export_job(
            AwsAccountId=AWS_ACCOUNT_ID,
            AssetBundleExportJobId=FOLDER_ID,
            ExportFormat='QUICKSIGHT_JSON',
            # IncludeFolderMembers='ONE_LEVEL',
            IncludeFolderMembers='RECURSE',
            IncludeAllDependencies=True,
            IncludePermissions=True,
            ResourceArns=[f'arn:aws:quicksight:{AWS_REGION}:{AWS_ACCOUNT_ID}:folder/{PARENT_FOLDER_ID}']
        )

        # Monitor job status
        status = 'QUEUED_FOR_IMMEDIATE_EXECUTION'
        while status in ['QUEUED_FOR_IMMEDIATE_EXECUTION', 'IN_PROGRESS']:
            response = quicksight.describe_asset_bundle_export_job(
                AwsAccountId=AWS_ACCOUNT_ID,
                AssetBundleExportJobId=FOLDER_ID
            )
            status = response['JobStatus']
            logger.info(f"Export job status: {status}")
            time.sleep(5)

        return response['DownloadUrl']

    except ClientError as e:
        logger.error(f"AWS error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def download_and_extract(download_url):
    """Download and extract the asset bundle"""
    try:
        http = urllib3.PoolManager()
        logger.info("Downloading asset bundle...")
        qs_file_content = http.request('GET', download_url).data
        
        with open(TEMP_ZIP, "wb") as qs_file:
            qs_file.write(qs_file_content)
        
        logger.info("Extracting asset bundle...")
        with zipfile.ZipFile(TEMP_ZIP, "r") as zip_ref:
            zip_ref.extractall(TEMP_DIR)

    except Exception as e:
        logger.error(f"Error in download and extract: {e}")
        raise

def modify_file_permissions(filepath):
    """Modify permissions for a single file"""
    try:
        with open(filepath, "r") as file:
            content = json.load(file)

        if 'permissions' in content:
            content.pop('permissions')

        with open(filepath, "w") as file:
            json.dump(content, file, indent=4)
            
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error in file {filepath}: {e}")
        raise
    except IOError as e:
        logger.error(f"IO error while processing file {filepath}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while processing file {filepath}: {e}")
        raise

def modify_permissions():
    """Modify the permissions in all extracted files"""
    logger.info("Starting permission modification process...")
    
    if not os.path.exists(TEMP_DIR):
        logger.error(f"Directory {TEMP_DIR} does not exist")
        raise FileNotFoundError(f"Directory {TEMP_DIR} not found")

    try:
        file_count = 0
        for root, _, files in os.walk(TEMP_DIR):
            for filename in files:
                filepath = os.path.join(root, filename)
                modify_file_permissions(filepath)
                file_count += 1

        logger.info(f"Successfully modified permissions in {file_count} files")
        
    except Exception as e:
        logger.error(f"Failed to modify permissions: {e}")
        raise

def create_modified_bundle():
    """Create the modified asset bundle"""
    try:
        logger.info("Creating modified asset bundle...")
        os.makedirs(os.path.dirname(OUTPUT_ZIP), exist_ok=True)
        with zipfile.ZipFile(OUTPUT_ZIP, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(TEMP_DIR):
                for filename in files:
                    filepath = os.path.join(root, filename)
                    arcname = os.path.relpath(filepath, TEMP_DIR)
                    zipf.write(filepath, arcname)

    except Exception as e:
        logger.error(f"Error creating modified bundle: {e}")
        raise

def main():
    """Main execution function"""
    try:
        # Clean up any existing temporary files
        cleanup_temp_files()
        # Execute the workflow
        download_url = start_export_job()
        print(download_url)
        download_and_extract(download_url)
        modify_permissions()
        create_modified_bundle()

        logger.info("Process completed successfully!")

    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)
    finally:
        cleanup_temp_files()

if __name__ == "__main__":
    main()