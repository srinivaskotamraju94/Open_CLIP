import os
import sys
import asyncio
import argparse
#pip install aiohttp
import aiohttp
#pip install aiofiles
import aiofiles
from io import BytesIO
from typing import List, Tuple, Optional
from timeit import default_timer as timer
from datetime import timedelta
from PIL import Image
import requests



def read_image_urls(image_urls_filepath) :

    image_url_tuples = []

    with open(image_urls_filepath, "r") as f:
        for line in f:
            image_id, image_url = line.split(",")
            image_id = int(image_id.strip())
            image_url = image_url.strip()
            image_url_tuple = (image_id, image_url)
            image_url_tuples.append(image_url_tuple)

    return image_url_tuples
  


async def async_download_image(image_url_tuple , download_dir):

    image_id, image_url = image_url_tuple
    image_filename = f"{image_id}.jpg"
    image_filepath = os.path.join(download_dir, image_filename)
    #os.chdir(download_dir)
    async with aiohttp.ClientSession() as session:
        #processed_url = image_url + "?odnHeight=224&odnWidth=224&odnBg=ffffff"
        async with session.request(method='GET',url=image_url) as response:
            if response.status == 200:
                try : 
                  content = await response.read()
                  ImageBytes = BytesIO(content)
                  ImgFile = Image.open(ImageBytes).resize((224,224)).convert("RGB")
                  buf = BytesIO()
                  ImgFile.save(buf,format = 'JPEG')
                  byte_im = buf.getvalue()
                  async with aiofiles.open(image_filepath, "wb") as f:
                    await f.write(byte_im)

                except : 
                  pass
            else:
                print(f"Unable to download image {image_id} from {image_url}")
 


async def async_download_images(image_url_tuples: List[Tuple[int, str]],download_dir):

    coroutines = [
        async_download_image(image_url_tuple=image_url_tuple,
                             download_dir=download_dir)
        for image_url_tuple in image_url_tuples if image_url_tuple[1] != "None"
    ]

    await asyncio.gather(*coroutines)
    
    

    
    
    
if __name__ == "__main__" :
  
      parser = argparse.ArgumentParser(
        description="Download Pixabay royalty-free images.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    
      parser.add_argument("--image-urls-filepath",
                          type=str,
                          help="A file which should have the following : ImageId , URL to download",
                         )
      
      parser.add_argument("--download-dir",
                          type=str,
                          help="The directory for saving the downloaded images."
                         )
      
      argv = parser.parse_args()
      
      image_urls_filepath = argv.image_urls_filepath
      download_dir = argv.download_dir
      image_url_tuples = read_image_urls(image_urls_filepath)
      
      print("Downloading images...")
      start = timer()
      
      # Python 3.7+
      if sys.version_info >= (3, 7):
        asyncio.run(
          async_download_images(image_url_tuples=image_url_tuples,
                                download_dir=download_dir))
        
      # Python 3.5-3.6
      else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
          async_download_images(image_url_tuples=image_url_tuples,
                                download_dir=download_dir))
      
      
      end = timer()
      print(f"Download Time Elapsed: {timedelta(seconds=end - start)}")

    
    

  
  
