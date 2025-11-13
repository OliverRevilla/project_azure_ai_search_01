from azure.storage.filedatalake import DataLakeServiceClient,FileSystemClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from typing import Dict
from azure.storage.blob import BlobServiceClient, PublicAccess

class AzureDataLakeGen2():
    def __init__(self, connection_string):
        self.connection_string = connection_string
        
    def get_authenticacion(self):
        try:
            self.service_client = DataLakeServiceClient.from_connection_string(self.connection_string)
            print("Autenticación exitosa")
        except Exception as e:
            print(f"Error en la autenticación: {e}")
    
    def create_container(self, container_name):
        try:
            self.get_authenticacion()
            container_client = self.service_client.create_file_system(container_name)
            print(f"Contenedor '{container_name}' creado exitosamente")
        except ResourceExistsError:
            print(f"El contenedor '{container_name}' ya existe")
        except Exception as e:
            print(f"Error al crear el contenedor: {e}")
    
    def create_or_replace_container(self, container_name):
        try:
            self.get_authenticacion()
            if self.service_client.get_file_system_client(container_name).exists():
                print(f"El contenedor '{container_name}' ya existe")
            else:
                container_client = self.service_client.create_file_system(container_name)
        except Exception as e:
            print(f"Error al crear o reemplazar el contenedor: {e}")
    
    def delete_container(self, container_name):
        try:
            self.get_authenticacion()
            self.service_client.delete_file_system(container_name)
            print(f"Contenedor '{container_name}' eliminado exitosamente")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' no existe")
        except Exception as e:
            print(f"Error al eliminar el contenedor: {e}")

    def list_containers(self):
        try:
            self.get_authenticacion()
            containers = self.service_client.list_file_systems()
            for container in containers:
                print(container.name)
        except Exception as e:
            print(f"Error al listar los contenedores: {e}")
    
    def list_directories(self, container_name):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            paths = container_client.get_paths()
            for path in paths:
                print(path.name)
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' no existe")
        except Exception as e:
            print(f"Error al listar los directorios: {e}")

    def list_files(self, container_name, directory_name):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            paths = container_client.get_paths(path=directory_name)
            for path in paths:
                print(path.name)
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' o el directorio '{directory_name}' no existe")
        except Exception as e:
            print(f"Error al listar los archivos: {e}")

    def create_directory(self, container_name, directory_name):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            directory_client = container_client.create_directory(directory_name)
            print(f"Directorio '{directory_name}' creado exitosamente en el contenedor '{container_name}'")
        except ResourceExistsError:
            print(f"El directorio '{directory_name}' ya existe en el contenedor '{container_name}'")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' no existe")
        except Exception as e:
            print(f"Error al crear el directorio: {e}")
    
    def create_or_replace_directory(self, container_name, directory_name):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            directory_client = container_client.get_directory_client(directory_name)
            if directory_client.exists():
                print(f"El directorio '{directory_name}' ya existe en el contenedor '{container_name}'")
            else:
                container_client.create_directory(directory_name)
                print(f"Directorio '{directory_name}' creado exitosamente en el contenedor '{container_name}'")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' no existe")
        except Exception as e:
            print(f"Error al crear o reemplazar el directorio: {e}")    

    def create_hierarchical_directory(self, container_name, directory_path: list):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            directory_client = container_client.create_directory(directory_path)
            print(f"Directorio jerárquico '{directory_path}' creado exitosamente en el contenedor '{container_name}'")
        except ResourceExistsError:
            print(f"El directorio jerárquico '{directory_path}' ya existe en el contenedor '{container_name}'")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' no existe")
        except Exception as e:
            print(f"Error al crear el directorio jerárquico: {e}")

    def delete_directory(self, container_name, directory_name):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            directory_client = container_client.get_directory_client(directory_name)
            directory_client.delete_directory()
            print(f"Directorio '{directory_name}' eliminado exitosamente del contenedor '{container_name}'")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' o el directorio '{directory_name}' no existe")
        except Exception as e:
            print(f"Error al eliminar el directorio: {e}")

    def get_empty_directory(self, container_name, directory_name):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            directory_client = container_client.get_directory_client(directory_name)
            paths = container_client.get_paths(path=directory_name)
            for path in paths:
                if not path.is_directory:
                    file_client = container_client.get_file_client(path.name)
                    file_client.delete_file()
            print(f"Directorio '{directory_name}' vaciado exitosamente en el contenedor '{container_name}'")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' o el directorio '{directory_name}' no existe")
        except Exception as e:
            print(f"Error al vaciar el directorio: {e}")
    
    def delete_file(self, container_name, file_path):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            file_client = container_client.get_file_client(file_path)
            file_client.delete_file()
            print(f"Archivo '{file_path}' eliminado exitosamente del contenedor '{container_name}'")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' o el archivo '{file_path}' no existe")
        except Exception as e:
            print(f"Error al eliminar el archivo: {e}")
    
    def file_exists(self, container_name, file_path):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            file_client = container_client.get_file_client(file_path)
            exists = file_client.exists()
            if exists:
                print(f"El archivo '{file_path}' existe en el contenedor '{container_name}'")
            else:
                print(f"El archivo '{file_path}' no existe en el contenedor '{container_name}'")
            return exists
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' no existe")
            return False
        except Exception as e:
            print(f"Error al verificar la existencia del archivo: {e}")
            return False
        
    def to_csv_file(self, container_name, file_path, dataframe):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            file_client = container_client.get_file_client(file_path)            
            csv_data = dataframe.to_csv(index=False).encode('utf-8')
            if self.file_exists(container_name, file_path):
                self.delete_file(container_name, file_path)
            else:
                pass
            file_client.upload_data(csv_data, overwrite=True)
            print(f"Archivo '{file_path}' subido exitosamente al contenedor '{container_name}'")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' no existe")
        except Exception as e:
            print(f"Error al subir el archivo: {e}")
            
    def get_updated_date_file(self, container_name, file_path):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            file_client = container_client.get_file_client(file_path)
            properties = file_client.get_file_properties()
            last_modified = properties.last_modified
            print(f"El archivo '{file_path}' fue modificado por última vez el {last_modified}")
            return last_modified
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' o el archivo '{file_path}' no existe")
            return None
        except Exception as e:
            print(f"Error al obtener la fecha de actualización del archivo: {e}")
            return None

    def download_file(self, container_name, file_path, download_path):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            file_client = container_client.get_file_client(file_path)
            download = file_client.download_file()
            with open(download_path, "wb") as local_file:
                download.readinto(local_file)
            print(f"Archivo '{file_path}' descargado exitosamente a '{download_path}'")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' o el archivo '{file_path}' no existe")
        except Exception as e:
            print(f"Error al descargar el archivo: {e}")
    
    def grant_access_directory(self, container_name, directory_name, permission, expiry_time):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            directory_client = container_client.get_directory_client(directory_name)
            sas_token = directory_client.generate_shared_access_signature(permission=permission, expiry=expiry_time)
            print(f"SAS token generado exitosamente para el directorio '{directory_name}': {sas_token}")
            return sas_token
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' o el directorio '{directory_name}' no existe")
            return None
        except Exception as e:
            print(f"Error al generar el SAS token: {e}")
            return None
        
    def change_anonymous_access_container(self, container_name):
        try:
            self.get_authenticacion()
            container_client = self.service_client.get_file_system_client(container_name)
            container_client.set_file_system_access_policy(
                signed_identifiers = {},
                public_access="container"
                )
                
            print(f"El nivel de acceso anónimo del contenedor '{container_name}' ha sido cambiado a Public access'")
        except ResourceNotFoundError:
            print(f"El contenedor '{container_name}' no existe")
        except Exception as e:
            print(f"Error al cambiar el nivel de acceso anónimo: {e}")