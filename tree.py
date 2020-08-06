import os
import pandas as pd
import re
import pyarrow.parquet as pq
import spacy
import numpy as np
import BLS_Request
from scipy import spatial
class node:
    def __init__(self, code, vector, children):
        self.code = code
        self.vector = vector
        self.children = children
    def __str__(self):
        return str(self.code)

path = str(os.path.dirname(os.path.realpath(__file__)))
punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
exceptionWords = ["excluding","except", "other_than","not","Inputs"]
def readNAPCS():
    filePath = os.path.join(path,"NAPCS-SCPAN-2017-Structure-V1-eng.csv")
    return pd.read_csv(filePath,encoding="iso8859_15")

def createBLSDataFrame():
    # Gets the group labels using the BLS_Request library.
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcInd","groupLabels")
    # Gets the item labels using the BLS_Request library.
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcLRef","labels")
    # Creates the paths for the for the item labels and the group labels
    newPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("pcLRef",BLS_Request.getAllFilesInDirectory("pcLRef")))
    newGroupPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("pcInd",BLS_Request.getAllFilesInDirectory("pcInd")))
    # Modifies the row headers for the two data frames.
    newGroupFrame = readParquet(newGroupPath)
    newDataFrame = readParquet(newPath)
    # Merges the two dataframes using a left join.
    mergeLeft = pd.merge(left=newGroupFrame,right=newDataFrame,how='left',left_on=0,right_on=0)
    # Gets the group labels using the BLS_Request library.
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpGrp","groupLabels")
    # Gets the item labels using the BLS_Request library.
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpLRef","labels")
    # Creates the paths for the for the item labels and the group labels
    newPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("wpLRef",BLS_Request.getAllFilesInDirectory("wpLRef")))
    newGroupPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("wpGrp",BLS_Request.getAllFilesInDirectory("wpGrp")))
    # Modifies the row headers for the two data frames.
    newGroupFrame = readParquet(newGroupPath)
    newDataFrame = readParquet(newPath)
    # Merges the two dataframes using a left join.
    mergeRight = pd.merge(left=newGroupFrame,right=newDataFrame,how='left',left_on=0,right_on=0)
    BLS_DataFrame = mergeLeft.append(mergeRight)
    BLS_DataFrame = BLS_DataFrame.drop(0)
    BLS_DataFrame = BLS_DataFrame.rename(columns={0:"code_1","1_x":"code_1_name","1_y":"code_2",2:"code_2_name"})
    BLS_DataFrame["combinedCodes"] = BLS_DataFrame["code_1"] + BLS_DataFrame["code_2"]
    return BLS_DataFrame

#Reads a parquet file and turns it into pyarrow table, then .to_pandas() converts the table to a dataframe.
def readParquet(fileName):
    # - fileName: (String) The name of the parquet file to be read and converted from parquet to pandas.
    return pq.read_table(fileName).to_pandas()

# Modifies the row headers. Takes the column titles from the first row in the dataframe and renames the column index titles with the content. 
def changeRowHeaders(dataFrame):
    dfList = dataFrame.values.tolist()
    for i in range(0,len(dfList[0])):
        dataFrame = dataFrame.rename(columns = {i:dfList[0][i]})
    return dataFrame

def mainDF():
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcCur","Current")
    newPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("pcCur",BLS_Request.getAllFilesInDirectory("pcCur")))
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpCur","Current")
    newPath1 = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("wpCur",BLS_Request.getAllFilesInDirectory("wpCur")))
    dataFrame = readParquet(newPath)
    dataFrame1 = readParquet(newPath1)
    dataFrame = dataFrame.append(dataFrame1)
    dataFrame = changeRowHeaders(dataFrame).drop(0)
    dataFrame["combinedCodes"] = dataFrame["series_id"].str[3:]
    return dataFrame


def getBLSFormatted():
    blsDF = createBLSDataFrame()
    dataFrame = mainDF()
    dataFrame = pd.merge(left=dataFrame,right=blsDF,how='left',left_on="combinedCodes",right_on="combinedCodes")
    dataFrame = dataFrame.drop(['combinedCodes',"year","period","value","footnote_codes"],axis=1)    
    dataFrame = dataFrame.drop_duplicates()
    return dataFrame

def removeComprise(string):
    if "comprises" in string:
        string = string.split("comprises")
        return string[1].strip()
    return string

def convertToVector(string):
    doc = nlp(string)
    c = np.zeros([300])
    for token in doc:
        c += token.vector
    return c

def removeExceptions(inputString):
    if "Input to stage" in inputString:
        string = string.replace("Input to stage ","")
        string = string[2:]
    startIndex = 0
    endIndex = 0
    splitStr = inputString.split()
    for i in splitStr:
        if i in exceptionWords:
            startIndex = splitStr.index(i)
            for j in range(startIndex,len(splitStr)):
                if "." in splitStr[j] or "," in splitStr[j]:
                    endIndex = j
                    break
        if startIndex != 0 and endIndex != 0:
            del splitStr[startIndex:endIndex+1]
            startIndex = 0 
            endIndex = 0
    return inputString

def prepString(rows):
    string = removeComprise(rows["Class definition"])
    string = string.replace("mfg","manufacturing")
    string = string.replace(", n.e.c.",".")
    string = string.replace("other than","other_than")
    if any(word in string for word in exceptionWords):
        string = removeExceptions(string)
    string = re.sub("[\(\[].*?[\)\]]", "", string)
    string = string.lower()
    for i in string:
        if i in punctuations:
            string = string.replace(i,"")
    string = string.split(" ")
    string = " ".join(list(dict.fromkeys(string)))
    string = string.replace("  "," ")
    #print("PROCESSED STRING : " + str(string))
    return string

def prepStringNotInRow(string):
    string = string.replace("mfg","manufacturing")
    string = string.replace(", n.e.c.",".")
    string = string.replace("other than","other_than")
    if any(word in string for word in exceptionWords):
        string = removeExceptions(string)
    string = re.sub("[\(\[].*?[\)\]]", "", string)
    string = string.lower()
    for i in string:
        if i in punctuations:
            string = string.replace(i,"")
    string = string.split(" ")
    string = " ".join(list(dict.fromkeys(string)))
    string = string.replace("  "," ")
    #print("PROCESSED STRING : " + str(string))
    return string

def checksForGPE(string):
    doc = nlp(string)
    cutWords = ["rv", "subprimal cuts"]
    containsGPE = False
    for ent in doc.ents:
        if ent.label_ == "LOC" and ent.text not in cutWords:
            #print(string + " (" + ent.text + ") " + " CONTAINS LOC")
            containsGPE = True
    if containsGPE == False:
        return string
    else:
        return 0

def findMostSimilar(childDictionary, comparision_term):
    mostSimilarVectorCode = ""
    mostSimilarVectorValue = 0
    for i in childDictionary.keys():
        temp = 1 - spatial.distance.cosine(childDictionary[i].vector, comparision_term)
        if temp > mostSimilarVectorValue:
            mostSimilarVectorValue = temp
            mostSimilarVectorCode = childDictionary[i]
    return mostSimilarVectorCode

nlp = spacy.load("en_core_web_lg")
NAPCSdf = readNAPCS()
NAPCSdfTemp = NAPCSdf[NAPCSdf["Level"]==1]
firstRow = {}
for row in NAPCSdfTemp.iterrows():
    firstRow[int(row[1]["Code"])] = node(int(row[1]["Code"]),convertToVector(prepString(row[1])),{})

NAPCSdfTemp = NAPCSdf[NAPCSdf["Level"]==2]
NAPCSdfTemp = NAPCSdfTemp["Code"].tolist()   
for i in firstRow.keys():
    for j in NAPCSdfTemp:
        if str(j)[:3] == str(i):
            firstRow[i].children[int(j)] = node(int(j),convertToVector(prepString(NAPCSdf[NAPCSdf["Code"]==j].iloc[0])),{})

NAPCSdfTemp = NAPCSdf[NAPCSdf["Level"]==3]
NAPCSdfTemp = NAPCSdfTemp["Code"].tolist()   
for i in firstRow.keys():
    for k in firstRow[i].children.keys():
        for j in NAPCSdfTemp:
            if str(j)[:5] == str(k):
                firstRow[i].children[int(k)].children[int(j)] = node(int(j),convertToVector(prepString(NAPCSdf[NAPCSdf["Code"]==j].iloc[0])),{})

NAPCSdfTemp = NAPCSdf[NAPCSdf["Level"]==4 ]
NAPCSdfTemp = NAPCSdfTemp["Code"].tolist()   
for i in firstRow.keys():
    for k in firstRow[i].children.keys():
        for l in firstRow[i].children[k].children.keys():
            for j in NAPCSdfTemp:
                if str(j)[:6] == str(l):
                    firstRow[i].children[int(k)].children[int(l)].children[int(j)] = node(int(j),convertToVector(prepString(NAPCSdf[NAPCSdf["Code"]==j].iloc[0])),{})

nlp = spacy.load("en_core_web_lg")
blsDF = getBLSFormatted()
blsDF["code_1_name"] = blsDF["code_1_name"].astype(str)
blsDF["code_2_name"] = blsDF["code_2_name"].astype(str)
resultFrame = []
for blsRows in blsDF.iterrows(): 
    string = blsRows[1]["code_2_name"]
    string = checksForGPE(prepStringNotInRow(string))
    if string != 0:
        string = convertToVector(string)
        currentRow = firstRow
        currentNode = ""
        while isinstance(currentRow,dict):
            currentRow = findMostSimilar(currentRow, string)
            if currentRow != "":
                currentNode = currentRow
                currentRow = currentRow.children
        if currentNode != "":
            resultFrame.append([currentNode.code,blsRows[1]["series_id"],(1 - spatial.distance.cosine(currentNode.vector, string))])
        else:
            resultFrame.append(["NO MATCH",blsRows[1]["series_id"],0])

NAPCSdf = NAPCSdf.drop(['Level','Hierarchical structure'],axis=1)
blsDF = blsDF.drop(['code_1','code_2'],axis=1)
tempDF = pd.DataFrame(resultFrame,columns=["Code","series_id","similarity"])
fullDF = pd.merge(tempDF,blsDF, on="series_id")
fullDF = pd.merge(fullDF,NAPCSdf, on="Code")
fullDF = fullDF.rename(columns={"series_id":"BLS Series ID","Code":"NACPS Code", "similarity":"Similarity","code_1_name":"Industry/Group Name","code_2_name":"Product/Item Name"})
fullDF = fullDF.sort_values(by="Similarity", ascending=False)
newPath = os.path.join(path,"help.csv")
fullDF.to_csv(newPath, index=False)
