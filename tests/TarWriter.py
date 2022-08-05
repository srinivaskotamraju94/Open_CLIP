import os 
import json 
import ast
import pandas as pd
import argparse
import webdataset as wbs
from PIL import Image



def gettarfiles(filepath,ImagePath,DestinationPath) : 
  os.chdir(DestinationPath)
  JsonFileNo = 0
  
  for JsonFile in os.listdir(filepath):
    print("JsonFile being processed is {}".format(JsonFile))
    AllItemList = []
    InputFile = [json.loads(line) for line in open(os.path.join(filepath,JsonFile),'r')]
    
    for value in range(len(InputFile)) :
      TempList = []
      TempList.append(InputFile[value]['item_id'])
      TempList.append(InputFile[value]['product_name'])
      AllItemList.append(TempList)
      
    ItemDF = pd.DataFrame(AllItemList,columns = ['ItemId','ProductName'])
      
    TarFile = wbs.TarWriter("ClipImage{0:03d}.tar".format(JsonFileNo))
    
    for ind in ItemDF.index :
      basename = ItemDF['ItemId'][ind]
      filename = basename + '.jpg'
      try : 
        with open(os.path.join(ImagePath,filename),'rb') as stream :
          Image = stream.read()
        Caption = ItemDF['ProductName'][ind] 
        dict = {
          "__key__":basename,
          "jpg":Image,
          "txt":Caption}
        
        Tarfile.write(dict)
      
      except Exception as ex :
        print(ex)
     
    TarFile.close()
    print("TarFile {} is Completed".format(JsonFileNo))
      
    JsonFileNo = JsonFileNo + 1
    
if __name__ == "__main__" :
  parser = argparse.ArgumentParser(
    description="Download Pixabay royalty-free images.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
  parser.add_argument("--filepath",
                          type=str,
                          help="The directory of the JSON Files",
                     )
      
  parser.add_argument("--ImagePath",
                      type=str,
                      help="The directory of the downloaded images."
                     )  
    
  parser.add_argument("--DestinationPath",
                      type=str,
                      help="Directory to save Tar Files"
                     )
  
  argv = parser.parse_args()
  
  filepath = argv.filepath
  ImagePath = argv.ImagePath 
  DestinationPath = argv.DestinationPath 
  
  gettarfiles(filepath,ImagePath,DestinationPath)
      
  
