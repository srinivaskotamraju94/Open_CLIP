import os 
import json 
#os.chdir("/Users/s0k09e3/Downloads")
import ast
import pandas as pd
import argparse

#folder_dir = "/rapids/notebooks/host/JsonFiles"

def gettextfiles(filepath) : 
    
    AllItemList = []
    
    for jsonfiles in os.listdir(filepath) : 
        
        InputFile = [json.loads(line) for line in open(os.path.join(filepath,jsonfiles),'r')]
        
        # Pre-Processing 01 - Making Sure all the Item IDs have a Product Name and Asset 
        
        ItemId = []
        ProductName = []
        Asset = []
        Asset_url = []

        for i in range(len(InputFile)) :
            InputFile[i].setdefault('product_name','NA')
            InputFile[i].setdefault('assets','NA')
            
            if (InputFile[i]['product_name'] != 'NA' and InputFile[i]['assets'] != 'NA') : 
                ItemId.append(InputFile[i]['itemid'])
                ProductName.append(InputFile[i]['product_name'])
                Asset.append(InputFile[i]['assets'])


        
        for i in range(len(Asset)) : 
            assets_list = json.loads(Asset[i])
            properties_list = []
            for k in range(len(assets_list)) : 
                for key in assets_list[k] : 
                    if key == 'properties' :
                        properties_list.append(assets_list[k][key])
                    
            for j in range(len(properties_list)) : 
                for key in properties_list[j] : 
                    if key == 'assetType' : 
                        if properties_list[j][key] == 'PRIMARY':
                            Asset_url.append(properties_list[j]['assetUrl'])
                    



        for item in range(len(ItemId)) :
            temp_list = [] 
            assets_list = json.loads(Asset[item])
            properties_list = []
            
            for k in range(len(assets_list)) : 
                for key in assets_list[k] : 
                    if key == 'properties' : 
                        properties_list.append(assets_list[k][key])
    
            for j in range(len(properties_list)) : 
            
                for val in properties_list[j] : 
                    if val == 'assetType' : 
                        if properties_list[j][val] == 'PRIMARY' : 
                            temp_list.append(ItemId[item])
                            temp_list.append(ProductName[item])
                            temp_list.append(properties_list[j]['assetUrl'])
                            AllItemList.append(temp_list)
                            
                            
    return AllItemList
                    


def dataframe_func(AllItemList) :
    ItemDF = pd.DataFrame(AllItemList,columns = ['ItemId','ProductName','ProductURL'])
    #pd.options.display.max_colwidth = 200
    return ItemDF
    #ItemDF.head()



def ImageURLFile(ItemDataFrame) :
    
    ImageURLFile = open('/rapids/notebooks/host/ImgUrlFile', 'w')

    for value in ItemDataFrame.index : 
        ItemId = ItemDataFrame['ItemId'][value]
        ItemURL = ItemDataFrame['ProductURL'][value]
    
        ImageURLFile.write("{},{}\n".format(ItemId,ItemURL))

    ImageURLFile.close()


def ImageCaptionFile(ItemDataFrame) :
    os.chdir("/rapids/notebooks/host/CaptionFiles")
    
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
        description="Download Pixabay royalty-free images.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
      parser.add_argument("--filepath",type=str,help="A file which should have the following : ImageId , URL to download",)
        
      argv = parser.parse_args()
    
      filepath = argv.filepath 
      AllItemList = gettextfiles(filepath)
      ItemDF = dataframe_func(AllItemList)
      
      ImageURLFile(ItemDF)
      ImageCaptionFile(ItemDF)
