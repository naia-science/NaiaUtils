from io import BytesIO
import pandas as pd
from google.cloud import storage
import os

def upload_dirs_to_gcp(bucket_name, project, jsonAuthPath, downloadDirPath):
    storage_client = storage.Client.from_service_account_json(jsonAuthPath, project=project)

    # Obtenir le bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Obtenez la liste de tous les fichiers dans le dossier
    files = os.listdir(downloadDirPath)

    # Parcourez chaque fichier ET dossier dans le dossier
    for root, _, files in os.walk(downloadDirPath):
        for file_name in files:
            # Construct the full local path
            local_path = os.path.join(root, file_name)
        
            # Create relative path for the object in GCS
            relative_path = os.path.relpath(local_path, downloadDirPath)

            # Create blob object and upload to GCS
            blob = bucket.blob(relative_path)
            blob.upload_from_filename(local_path)

            print(f"Uploaded {local_path} to gs://{bucket_name}/{relative_path}")

    print("Tous les fichiers ont été téléchargés avec succès.")

def upload_files_to_gcp(bucket_name, project, jsonAuthPath, downloadDirPath):
    storage_client = storage.Client.from_service_account_json(jsonAuthPath, project=project)

    # Obtenir le bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Obtenez la liste de tous les fichiers dans le dossier
    files = os.listdir(downloadDirPath)

    # Parcourez chaque fichier dans le dossier
    for file_name in files:
        # Chemin complet du fichier
        file_path = os.path.join(downloadDirPath, file_name)
    
        # Nom du fichier sur GCP (vous pouvez garder le même nom ou modifier selon vos besoins)
        image_name_on_gcp = file_name
    
        # Définir le blob dans le bucket
        blob = bucket.blob(image_name_on_gcp)
    
        # Définir le type de contenu de l'image (vous pouvez modifier cela en fonction du type de fichier)
        #blob.content_type = "jpg"
        blob.content_type = os.path.splitext(image_name_on_gcp)[1]
    
        # Télécharger l'image depuis le chemin local vers GCP
        with open(file_path, "rb") as f:
            blob.upload_from_file(f)
    
        print("Fichier '{}' uploadé avec succès.".format(file_name))

    print("Tous les fichiers ont été uploadés avec succès.")

def download_gcp(bucket_name, project, jsonAuthPath, downloadDirPath):
    
    #if path does not exist, create it
    if not os.path.exists(downloadDirPath):
        os.makedirs(downloadDirPath)

    # Créer une instance de client Storage à partir du fichier JSON contenant les clés d'identification de service
    storage_client = storage.Client.from_service_account_json(jsonAuthPath, project=project)

    # Obtenir le bucket
    bucket = storage_client.get_bucket(bucket_name)


    # Obtenez la liste de tous les blobs dans le bucket
    blobs = bucket.list_blobs()

    # Parcourez chaque blob dans le bucket
    for blob in blobs:
        # Obtenez le nom du blob
        blob_name = blob.name
    
        # Téléchargez le blob dans le dossier de téléchargement avec le même nom

        download_path = os.path.join(downloadDirPath, blob_name)
        #créer le dossier si il n'existe pas
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        blob.download_to_filename(download_path)
    

    print("Tous les fichiers ont été téléchargés avec succès.")

# add a main function to run the code
if __name__ == "__main__":

    # parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Upload files to GCP")

    parser.add_argument("--bucket_name", type=str, help="Name of the bucket")
    parser.add_argument("--project", type=str, help="Name of the project")
    parser.add_argument("--jsonAuthPath", type=str, help="Path to the json authentication file")
    parser.add_argument("--downloadDirPath", type=str, help="Path to the directory to upload or download")
    parser.add_argument("--upload", type=bool, help="True to upload, False to download")

    args = parser.parse_args()

    # all arguments are required, check for None
    if None in vars(args).values():
        print("Please privide all required arguments. Run with -h for help.")
        exit(1)

    # change upload arg to a boolean
    args.upload = True if args.upload == "True" else False    

    if args.upload:
        print("Uploading files to GCP...")
        upload_dirs_to_gcp(args.bucket_name, args.project, args.jsonAuthPath, args.downloadDirPath)

    else:
        print("Downloading files from GCP...")
        download_gcp(args.bucket_name, args.project, args.jsonAuthPath, args.downloadDirPath)
    