import os
import sys
import asyncio
import argparse
#pip install aiohttp
import aiohttp
from aiohttp import ClientSession, ClientResponseError, TCPConnector, ClientConnectorError, ClientError
#pip install aiofiles
import aiofiles
from io import BytesIO
#from typing import List, Tuple, Optional
from timeit import default_timer as timer
from datetime import timedelta
from PIL import Image
import requests
import backoff
import logging


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
  
SEMA = asyncio.BoundedSemaphore(500)
HTTP_STATUS_CODES_TO_RETRY = {500, 502, 503, 504}


def timeout_handler(details):

    logging.warning("Retrying url: {} ({} retry)".format(details['args'][0],details['tries']+1))
    
    
def error_code_handler(e):

    return e.code not in HTTP_STATUS_CODES_TO_RETRY

def backoff_hdlr_2(details):
    print(details)

@backoff.on_exception(backoff.expo, asyncio.TimeoutError, max_tries=5,  on_backoff=timeout_handler)
@backoff.on_exception(backoff.expo, ClientResponseError, max_tries=5, on_backoff=backoff_hdlr_2, giveup=error_code_handler)


async def async_download_image(image_url_tuple,download_dir) :
    
    image_id, image_url = image_url_tuple
    processed_url = image_url + "?odnHeight=224&odnWidth=224&odnBg=ffffff"
    image_filename = f"{image_id}.jpg"
    image_filepath = os.path.join(download_dir, image_filename)

    async with aiohttp.ClientSession(raise_for_status=True,connector=aiohttp.TCPConnector(ssl=False, limit=100),trust_env=True) as session:
        
        async with SEMA :
            
            try : 
                async with session.request(method='GET',url=processed_url,timeout = 300) as response:
                    if response.status == 200:
                        try :
                            content = await response.read()
                            ImageBytes = BytesIO(content)
                            ImgFile = Image.open(ImageBytes).convert("RGB")
                            buf = BytesIO()
                            ImgFile.save(buf,format = 'JPEG')
                            byte_im = buf.getvalue()
                            async with aiofiles.open(image_filepath, "wb") as f:
                                await f.write(byte_im)
                    
                        except Exception as ex: 
                            print(ex)
                            pass
                    else :
                        print(f"Unable to download image {image_id} from {processed_url}")
                    
            except asyncio.TimeoutError as e:
                message = "Image download failed: Timeout Error for i5 url {}".format(url)
                logging.warning(message)
                return processed_url, 408, message
            except ClientResponseError as e:
                message = "Image download failed: Client Response Error for i5 url {}, status code {}".format(url,e.code)
                logging.warning(message)
                return processed_url, e.code, message
            except Exception as e:
                message = "Image download failed: Unknown Error for i5 url {}, ".format(url)
                logging.warning(message)
                logging.warning(e)
                return processed_url, -1, message


async def async_download_images(image_url_tuples,download_dir):

    coroutines = [
        async_download_image(image_url_tuple=image_url_tuple,download_dir = download_dir)
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
          async_download_images(image_url_tuples=image_url_tuples, download_dir = download_dir))
                                
        
      # Python 3.5-3.6
      else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
          async_download_images(image_url_tuples=image_url_tuples, download_dir = download_dir))
                             
      
      
      end = timer()
      print(f"Download Time Elapsed: {timedelta(seconds=end - start)}")

    
    

  
  
