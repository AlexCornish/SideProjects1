import os
import re
import pandas as pd
import BLS_Request
import spacy
import pyarrow.parquet as pq
import numpy as np
from scipy import spatial
path = str(os.path.dirname(os.path.realpath(__file__)))
punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
exceptionWords = ["excluding","except", "other_than","not"]

#Reads a parquet file and turns it into pyarrow table, then .to_pandas() converts the table to a dataframe.
def readParquet(fileName):
    # - fileName: (String) The name of the parquet file to be read and converted from parquet to pandas.
    return pq.read_table(fileName).to_pandas()

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

def readNAPCS():
    filePath = os.path.join(path,"NAPCS-SCPAN-2017-Structure-V1-eng.csv")
    dataFrame = pd.read_csv(filePath,encoding="iso8859_15")
    return changeRowHeaders(dataFrame)

def changeRowHeaders(dataFrame):
    dfList = dataFrame.values.tolist()
    for i in range(0,len(dfList[0])):
        dataFrame = dataFrame.rename(columns = {i:dfList[0][i]})
    return dataFrame

def removeExceptions(inputString):
    if "Inputs to stage" in inputString:
        inputString = inputString[17:]
    for i in exceptionWords:
        if i in inputString:
            inputString = inputString.split(i)[0]
    return inputString

def removeComprise(string):
    if "comprises" in string:
        string = string.split("comprises")
        return string[1].strip()
    return string

def prepString(rows):
    string = removeComprise(rows)
    string = string.replace("mfg","manufacturing")
    string = string.replace(", n.e.c.",".")
    string = string.replace("other than","other_than")
    string = removeExceptions(string)
    string = re.sub("[\(\[].*?[\)\]]", "", string)
    string = string.lower()
    for i in string:
        if i in punctuations:
            string = string.replace(i,"")
    string = string.split(" ")
    string = " ".join(list(dict.fromkeys(string)))
    string = string.replace("  "," ")
    doc = nlp(string)
    c = np.zeros([300])
    for token in doc:
        c += token.vector
    return c

def nNearestBLStoNAPCS(blsNumber, numberToReturn):
    dataFrame = comparisonBLS(blsNumber)
    dataFrame = dataFrame.sort_values(by="similarity", ascending=False)
    return dataFrame.head(numberToReturn)
    
def nNearestNAPCStoBLS(napcsNumber, numberToReturn):
    dataFrame = comparisonNAPCS(napcsNumber)
    dataFrame = dataFrame.sort_values(by="similarity", ascending=False)
    return dataFrame.head(numberToReturn)

def comparisonBLS(blsNumber):
    tempBLS = blsDF.loc[blsDF.series_id == blsNumber,"vector"].tolist()[0]
    tempDF["similarity"] = tempDF["vector"].apply(lambda x: 1 - spatial.distance.cosine(x, tempBLS))
    return tempDF

def comparisonNAPCS(NAPCSNumber):
    tempNAPCS = tempDF.loc[tempDF.Code == NAPCSNumber,"vector"].tolist()[0]
    blsDF["similarity"] = blsDF["vector"].apply(lambda x: 1 - spatial.distance.cosine(x, tempNAPCS))
    return blsDF

def convertToVector(string):
    doc = nlp(string)
    c = np.zeros([300])
    for token in doc:
        c += token.vector
    return c

nlp = spacy.load("en_core_web_lg")
tempDF = readNAPCS()
tempDF["Code"] = tempDF["Code"].astype(str)
blsDF = getBLSFormatted()
blsDF["code_2_name"] = blsDF["code_2_name"].astype(str)
preProcessingBool = int(input("Would you like to preprocess the labels before comparison? (0 for NO, 1 for YES): "))
if preProcessingBool == 1:
    tempDF["vector"] = tempDF["Class definition"].map(prepString)
    blsDF["vector"] = blsDF["code_2_name"].map(prepString)
else:
    tempDF["vector"] = tempDF["Class definition"].map(convertToVector)
    blsDF["vector"] = blsDF["code_2_name"].map(convertToVector)
