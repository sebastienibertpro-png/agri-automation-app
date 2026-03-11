import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials

class DriveUploader:
    def __init__(self, credentials_path=None, credentials_dict=None):
        self.credentials_path = credentials_path
        self.credentials_dict = credentials_dict
        self.service = self._authenticate()

    def _authenticate(self):
        scope = ['https://www.googleapis.com/auth/drive']
        try:
            if self.credentials_dict:
                creds = ServiceAccountCredentials.from_json_keyfile_dict(self.credentials_dict, scope)
            elif self.credentials_path and os.path.exists(self.credentials_path):
                creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_path, scope)
            else:
                raise Exception("Aucun identifiant de service compte fourni (fichier introuvable ou dictionnaire vide).")
            service = build('drive', 'v3', credentials=creds)
            return service
        except Exception as e:
            print(f"Erreur d'authentification Google Drive: {e}")
            return None

    def upload_file(self, file_path, folder_id):
        """Uploads a file to a specific Google Drive folder."""
        if not self.service:
            raise Exception("Service Drive non initialisé.")

        if not os.path.exists(file_path):
            raise Exception(f"Fichier introuvable: {file_path}")

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
            raise Exception(str(e))

    def download_latest_file_from_folder(self, folder_id, file_prefix, destination_path):
        """Finds the most recent file in a folder starting with prefix and downloads it."""
        if not self.service:
            raise Exception("Service Drive non initialisé.")
            
        try:
            # Search for files with the prefix in the folder, ordered by modified time descending
            query = f"'{folder_id}' in parents and name contains '{file_prefix}' and trashed = false"
            results = self.service.files().list(
                q=query,
                orderBy="modifiedTime desc",
                pageSize=1,
                fields="files(id, name)"
            ).execute()
            
            items = results.get('files', [])
            
            if not items:
                print(f"Aucun fichier trouvé pour '{file_prefix}' dans le dossier {folder_id}.")
                return False
                
            file_id = items[0]['id']
            file_name_remote = items[0]['name']
            print(f"Téléchargement de {file_name_remote} (ID: {file_id})...")
            
            request = self.service.files().get_media(fileId=file_id)
            import io
            from googleapiclient.http import MediaIoBaseDownload
            
            fh = io.FileIO(destination_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                
            print(f"Téléchargement terminé vers {destination_path}.")
            return True
            
        except Exception as e:
             print(f"Erreur lors du téléchargement: {e}")
             raise Exception(str(e))
