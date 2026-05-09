import boto3
import os

# Create S3 client
s3 = boto3.client('s3')

bucket_name = 'breta-you-tube-data-pipeline-bronze'

# Put your actual folder path here
folder_path = r'C:\Users\ADMIN\Documents\Projects\aws-youtube-trending-data-pipeline\data\kaggle'

print("Starting upload...")
print("Folder exists:", os.path.exists(folder_path))

for root, dirs, files in os.walk(folder_path):

    print("Current folder:", root)
    print("Files found:", files)

    for file in files:
        try:
            local_path = os.path.join(root, file)

            # Keep folder structure in S3
            s3_path = os.path.relpath(local_path, folder_path)

            print(f"Uploading: {local_path}")

            s3.upload_file(
                local_path,
                bucket_name,
                f'raw/{s3_path}'
            )

            print(f" Uploaded {file}")

        except Exception as e:
            print(f" Error uploading {file}")
            print(e)

print("Done.")