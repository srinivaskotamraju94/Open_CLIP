import os 
import json 
os.chdir("/Users/s0k09e3/Downloads")
import ast
import pandas as pd

InputFile = [json.loads(line) for line in open('Food-and-Beverages-072022_part-00000-17b673a7-79a2-4f9a-8ab4-b31d1c776353-c000.json','r')]

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
                    

AllItemList = []

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
                    


ItemDF = pd.DataFrame(AllItemList,columns = ['ItemId','ProductName','ProductURL'])
pd.options.display.max_colwidth = 200
ItemDF.head()


TxtFile = open('CLIP_Images', 'w')

for value in ItemDF.index : 
    
    ItemId = ItemDF['ItemId'][value]
    ItemURL = ItemDF['ProductURL'][value]
    
    TxtFile.write("{},{}\n".format(ItemId,ItemURL))

TxtFile.close()


for val in ItemDF.index : 
    ItemId = ItemDF['ItemId'][val]
    try :
        TextName = ItemId + '.txt'
        TxtFile = open(TextName, 'w')
        ImageCaption = ItemDF['ProductName'][val]
        TxtFile.write("{}\n".format(ImageCaption))
    
    except :
        pass
    
