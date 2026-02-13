import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials

class DriveUploader:
    def __init__(self, credentials_path):
        self.credentials_path = credentials_path
        self.service = self._authenticate()

    def _authenticate(self):
        scope = ['https://www.googleapis.com/auth/drive']
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_path, scope)
            service = build('drive', 'v3', credentials=creds)
            return service
        except Exception as e:
            print(f"Erreur d'authentification Google Drive: {e}")
            return None

    def upload_file(self, file_path, folder_id):
        """Uploads a file to a specific Google Drive folder."""
        if not self.service:
            print("Service Drive non initialisé. Upload annulé.")
            return None

        if not os.path.exists(file_path):
            print(f"Fichier introuvable: {file_path}")
            return None

        file_name = os.path.basename(file_path)
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(file_path, resumable=True)
        
        try:
            print(f"Upload de '{file_name}' vers le dossier Drive {folder_id}...", flush=True)
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"Succès ! Fichier uploadé avec ID: {file.get('id')}", flush=True)
            return file.get('id')
        except Exception as e:
            print(f"Erreur lors de l'upload: {e}", flush=True)
            return None
