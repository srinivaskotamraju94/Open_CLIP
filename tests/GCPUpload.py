import os 
from google.cloud import storage
import argparse

def Google_Cred(Credentials_filepath) : 
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Credentials_filepath
 

def upload_images(folder_dir) : 
  
  for Images in os.listdir(folder_dir) : 
    blob.upload_from_filename(os.path.join(folder_dir,Images))
    
    
if __name__ == "__main__" :
  
      parser = argparse.ArgumentParser(
        description="Download Pixabay royalty-free images.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    
      parser.add_argument("--Credentials-filepath",type=str,help="Path to Json File with Credentials",)
      
      parser.add_argument("--folder_dir",type=str,help="Path to Json File with Credentials",)
      
      argv = parser.parse_args()
      
      Credentials_filepath = argv.Credentials_filepath
      Google_Cred(Credentials_filepath)
      storage_client = storage.Client()
      bucket = storage_client.get_bucket('wmt-trust-and-safety')
      blob = bucket.blob('Food-and-Beverages-OpenClip/')
      
      folder_dir = argv.folder_dir 
      upload_images(folder_dir)
      
      
    



