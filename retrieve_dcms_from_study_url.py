#!/usr/bin/env python3
'''
A study url sent by diag lab:
https://novo.viewer.somatiq.ai/viewer/dicomjson?url=https://objectstore.e2enetworks.net/somatiqstorage/ris/prod/json/1.2.826.0.1.3680043.8.498.70081258420805385273299715195898522630.json?X-Amz-Algorithm=AWS4-HMAC-SHA256%26X-Amz-Credential=OT6XX9DMFHCN9QE8NOAM%2F20260313%2Fus-east-1%2Fs3%2Faws4_request%26X-Amz-Date=20260313T102703Z%26X-Amz-Expires=604800%26X-Amz-SignedHeaders=host%26X-Amz-Signature=6eadd1febf5717cca5358206212ba59f1abf8c35515b41c6de4d44e3857cfb54

https://novo.viewer.somatiq.ai/viewer/dicomjson?url=https://objectstore.e2enetworks.net/somatiqstorage/ris/prod/json/1.2.826.0.1.3680043.8.498.70081258420805385273299715195898522630.json?X-Amz-Algorithm=AWS4-HMAC-SHA256%26X-Amz-Credential=OT6XX9DMFHCN9QE8NOAM%2F20260313%2Fus-east-1%2Fs3%2Faws4_request%26X-Amz-Date=20260313T095624Z%26X-Amz-Expires=604800%26X-Amz-SignedHeaders=host%26X-Amz-Signature=4643704f34e36741d8d5d913ca6751581412ccf73a2a7fb056b54ba15fa4ef9e

Run this in browser developer console:
const entries = performance.getEntriesByType("resource");
const study_contents_entry = entries.filter(e => e.initiatorType.includes("fetch"))[0]
const study_contents_url = study_contents_entry.name
console.log(study_contents_url)

'''
import requests
import os
import json

def download_study(json_url):
    # 1. Fetch the JSON manifest
    print(f"Fetching manifest from: {json_url}")
    response = requests.get(json_url)
    if response.status_code != 200:
        print(f"Error fetching manifest: {response.status_code}, Details: {response.text}") # response.content is binary form
        return
    
    data = response.json()
    
    # Create a base folder for the study
    patient_name = data['studies'][0].get('PatientName', 'Unknown_Patient').replace('^', '_')
    study_uid = data['studies'][0].get('StudyInstanceUID', 'Unknown_Study')
    base_dir = f"study_{patient_name}_{study_uid[-6:]}"
    os.makedirs(base_dir, exist_ok=True)

    # 2. Iterate through Series and Instances
    count = 0
    for study in data.get("studies", []):
        for series in study.get("series", []):
            series_uid = series.get("SeriesInstanceUID", "Unknown_Series")
            series_dir = os.path.join(base_dir, series_uid[-6:])
            os.makedirs(series_dir, exist_ok=True)
            
            for instance in series.get("instances", []):
                raw_url = instance.get("url")
                if not raw_url:
                    continue
                
                # OHIF uses 'dicomweb:' prefix; we must remove it for requests to work
                clean_url = raw_url.replace("dicomweb:", "")
                
                # Use SOPInstanceUID for the filename
                sop_uid = instance.get("metadata", {}).get("SOPInstanceUID", f"img_{count}")
                file_path = os.path.join(series_dir, f"{sop_uid}.dcm")
                
                # 3. Download the actual DICOM file
                print(f"Downloading: {sop_uid[-8:]}...", end="\r")
                try:
                    img_response = requests.get(clean_url)
                    if img_response.status_code == 200:
                        with open(file_path, 'wb') as f:
                            f.write(img_response.content)
                        count += 1
                    else:
                        print(f"\nFailed to download {sop_uid}: {img_response.status_code}")
                except Exception as e:
                    print(f"\nError: {e}")

    print(f"\nFinished! Downloaded {count} files to folder: {base_dir}")

# --- EXECUTION ---
# Paste the full URL that returns the JSON you provided
# This results in error 403, access denied as it has encoded url string
#MANIFEST_URL = 'https://objectstore.e2enetworks.net/somatiqstorage/ris/prod/json/1.2.826.0.1.3680043.8.498.70081258420805385273299715195898522630.json?X-Amz-Algorithm=AWS4-HMAC-SHA256%26X-Amz-Credential=OT6XX9DMFHCN9QE8NOAM%2F20260313%2Fus-east-1%2Fs3%2Faws4_request%26X-Amz-Date=20260313T095624Z%26X-Amz-Expires=604800%26X-Amz-SignedHeaders=host%26X-Amz-Signature=4643704f34e36741d8d5d913ca6751581412ccf73a2a7fb056b54ba15fa4ef9e'
# This works... this is clean url
MANIFEST_URL = 'https://objectstore.e2enetworks.net/somatiqstorage/ris/prod/json/1.2.826.0.1.3680043.8.498.70081258420805385273299715195898522630.json?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=OT6XX9DMFHCN9QE8NOAM/20260313/us-east-1/s3/aws4_request&X-Amz-Date=20260313T095624Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=4643704f34e36741d8d5d913ca6751581412ccf73a2a7fb056b54ba15fa4ef9e'

download_study(MANIFEST_URL)

