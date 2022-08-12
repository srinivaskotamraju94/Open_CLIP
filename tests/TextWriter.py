# Code for creating Caption Files (.txt) Files  

import os 
import json 
#os.chdir("/Users/s0k09e3/Downloads")
import ast
import pandas as pd
import argparse

#folder_dir = "/rapids/notebooks/host/JsonFiles"

def gettextfiles(filepath) : 
    
    AllItemList = []
    
    for val in os.listdir(filepath) :
        if val != '.DS_Store' :
            InputFile = [json.loads(line) for line in open(os.path.join(filepath,val),'r')]
            
            for value in range(len(InputFile)) :
                TempList = []
                TempList.append(InputFile[value]['item_id'])
                TempList.append(InputFile[value]['product_name'])
                TempList.append(InputFile[value]['asset_url'])
                AllItemList.append(TempList)
    
   
    return AllItemList
                    


def dataframe_func(AllItemList) :
    ItemDF = pd.DataFrame(AllItemList,columns = ['ItemId','ProductName','ProductURL'])
    #pd.options.display.max_colwidth = 200
    return ItemDF
    #ItemDF.head()



def ImageURLFile(ItemDataFrame) :
    
    ImageURLFile = open('/rapids/notebooks/host/home/ImgUrlFile', 'w')

    for value in ItemDataFrame.index : 
        ItemId = ItemDataFrame['ItemId'][value]
        ItemURL = ItemDataFrame['ProductURL'][value]
    
        ImageURLFile.write("{},{}\n".format(ItemId,ItemURL))

    ImageURLFile.close()


def ImageCaptionFile(ItemDataFrame) :
    os.chdir("/rapids/notebooks/host/mnt/ClipImageText")
    
    for val in ItemDF.index : 
        ItemId = ItemDF['ItemId'][val]
        try :
            TextName = ItemId + '.txt'
            TxtFile = open(TextName, 'w')
            ImageCaption = ItemDF['ProductName'][val]
            TxtFile.write("{}\n".format(ImageCaption))
    
        except :
            pass
        
        
if __name__ == "__main__" :
  
      parser = argparse.ArgumentParser(
        description="Create Caption Files for CLIP Training.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
      parser.add_argument("--filepath",type=str,help="Filepath to Jsonfiles which should have the following : ImageId , URL to download",)
        
      argv = parser.parse_args()
    
      filepath = argv.filepath 
      AllItemList = gettextfiles(filepath)
      ItemDF = dataframe_func(AllItemList)
      
      ImageURLFile(ItemDF)
      ImageCaptionFile(ItemDF)
