import os 
import json 
import ast
import pandas as pd
import argparse

JsonFileNo = 0 

def gettarfiles(filepath,ImagePath) : 
  
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
      
    TarFile = TarWriter("ClipImage{0:03d}.tar".format(JsonFileNo))
    
    for ind in ItemDF.index :
      basename = Filtered_ImageList['ItemId'][ind]
      filename = basename + '.jpg'
      try : 
        with open(os.path.join(ImagePath,filename),'rb') as stream :
        Image = stream.read()
        Caption = ItemDF['ProductName'][index] 
        dict = {
          "__key__":basename,
          "jpg":Image,
          "txt":Caption
        }
      Tarfile.write(dict)
      except :
        print("{} is not available in the ImagePath".format(filename))
     
    TarFile.close()
      
    JsonFileNo = JsonFile + 1
      
  
