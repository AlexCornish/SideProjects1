import os
import re
import pandas as pd
import BLS_Request
import spacy
import pyarrow.parquet as pq
import numpy as np
import pyarrow as pa
import time
from IPython.display import display, HTML
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
    string = string.replace("  "," ")
    return convertToVector(string)

def convertToVector(string):
    doc = nlp(string)
    c = np.zeros([300])
    for token in doc:
        c += token.vector
    return c

def nNearestBLStoNAPCS(blsNumber, numberToReturn):
    dataFrame = comparisonBLS(blsNumber)
    dataFrame = dataFrame.sort_values(by="similarity", ascending=False)
    dataFrame = dataFrame.head(numberToReturn)
    dataFrame = dataFrame.drop(columns=["Class title", "vector"])
    dataFrame = dataFrame.reset_index(drop=True)
    pd.set_option('display.max_colwidth', None)
    return dataFrame
    
def nNearestNAPCStoBLS(napcsNumber, numberToReturn):
    dataFrame = comparisonNAPCS(napcsNumber)
    dataFrame = dataFrame.sort_values(by="similarity", ascending=False)
    dataFrame = dataFrame.head(numberToReturn)
    dataFrame = dataFrame.drop(columns=["vector"])
    dataFrame = dataFrame.reset_index(drop=True)
    pd.set_option('display.max_colwidth', None)
    return dataFrame

def comparisonBLS(blsNumber):
    blsNumber = blsNumber.strip()
    tempBLS = blsDF.loc[blsDF.series_id == blsNumber,"vector"].tolist()[0]
    blsDFRes = blsDF[blsDF["series_id"]==blsNumber].values.tolist()[0]
    print(blsDFRes[0] + ":      " + blsDFRes[1] + "    " + blsDFRes[2])
    tempDF["similarity"] = tempDF["vector"].apply(lambda x: 1 - spatial.distance.cosine(x, tempBLS))
    return tempDF

def comparisonNAPCS(NAPCSNumber):
    NAPCSNumber = NAPCSNumber.strip()
    tempNAPCS = tempDF.loc[tempDF.Code == NAPCSNumber,"vector"]
    tempNAPCS = tempNAPCS.tolist()[0]
    tempDFRes = tempDF[tempDF["Code"]==NAPCSNumber].values.tolist()[0]
    print(tempDFRes[0] + ":     " + tempDFRes[1] + "    " + tempDFRes[2])
    blsDF["similarity"] = blsDF["vector"].apply(lambda x: 1 - spatial.distance.cosine(x, tempNAPCS))
    return blsDF

def vectorStoragePathCreation():
    newPath = os.path.join(path,'RawData')
    # Checks if "newPath" exists and creates it if it doesn't
    if not os.path.exists(newPath):
        os.makedirs(newPath)
    vectorTableStoragePath = os.path.join(newPath,'VectorTables')
    # Checks if "newPath" exists and creates it if it doesn't
    if not os.path.exists(vectorTableStoragePath):
        os.makedirs(vectorTableStoragePath)
    return vectorTableStoragePath

def checkForBLS(path):
    blsPath = os.path.join(path,'BLSVectors.parquet')
    if not os.path.exists(blsPath):
        print("Current BLSVector.parquet not found. Creating new BLSVector.parquet...")
        blsDF = getBLSFormatted()
        blsDF["code_2_name"] = blsDF["code_2_name"].astype(str)
        blsDF["code_1_name"] = blsDF["code_1_name"].astype(str)
        blsDF["combinedCodes"] = blsDF["code_1_name"] + " " + blsDF["code_2_name"]
        blsDF["vector"] = blsDF["combinedCodes"].map(prepString)
        blsDF = blsDF.drop(columns=["code_1","code_2","combinedCodes"])
        table = pa.Table.from_pandas(blsDF)
        pq.write_table(table,blsPath)
        print("BLSVector.parquet created...")
        return blsDF
    else:
        print("BLSVector.parquet found...")
        return pq.read_table(blsPath).to_pandas()

def checkForNAPCS(path):
    napcsPath = os.path.join(path,'NAPCSVectors.parquet')
    if not os.path.exists(napcsPath):
        print("Current NAPCSVectors.parquet not found. Creating new NAPCSVectors.parquet...")
        napcsDF = readNAPCS()
        napcsDF["Code"] = napcsDF["Code"].astype(str)
        napcsDF["Class title"] = napcsDF["Class title"].astype(str)
        napcsDF["Class definition"] = napcsDF["Class definition"].astype(str)
        napcsDF["combinedCodes"] = napcsDF["Class title"] + " " + napcsDF["Class definition"]
        napcsDF["vector"] = napcsDF["combinedCodes"].map(prepString)
        napcsDF = napcsDF.drop(columns=["Level","Hierarchical structure","combinedCodes"])
        table = pa.Table.from_pandas(napcsDF)
        pq.write_table(table,napcsPath)
        print("NAPCSVectors.parquet created...")
        return napcsDF
    else:
        print("NAPCSVectors.parquet found...")
        return pq.read_table(napcsPath).to_pandas()

def convertToVectorWithWeightedValues(string, weights):
    doc = nlp(string)
    c = np.zeros([300])
    for token in range(0,len(doc)):
        c += (doc[token].vector * weights[token])
    return c

def parseEntry(userInput):
    # ++cattle cows beefalo -beef -meat --manufacturing
    entryArr = userInput.split(" ")
    weightsArr = []
    for i in entryArr:
        if "++" in i:
            weightsArr.append(3)
        elif "+" in i:
            weightsArr.append(2)
        elif "--" in i:
            weightsArr.append(-2)
        elif  "-" in i:
            weightsArr.append(-1)
        else:
            weightsArr.append(1)
    userInput = userInput.replace("+","").replace("-","")
    return convertToVectorWithWeightedValues(userInput,weightsArr)

def prototypeMatch(entry,searchMethod):
    blsORNAPCS = str(input("Is the code that you want to compare BLS or NAPCS? "))
    while blsORNAPCS != "BLS" and blsORNAPCS != "NAPCS":
        blsORNAPCS = str(input("Is the code that you want to compare BLS or NAPCS? "))
    if blsORNAPCS == "BLS":
        if searchMethod == "1":
            searchWord = str(input("What code would you like to search exactly for? "))
            searchWord = " " + searchWord + " "
            blsDF["combinedCodes"] = blsDF["code_1_name"] + " " + blsDF["code_2_name"]
            dataFrame = blsDF[blsDF["combinedCodes"].str.contains(searchWord)]
            dataFrame = dataFrame.drop(columns=["combinedCodes","vector"])
            dataFrame = dataFrame.reset_index(drop=True)
            pd.set_option('display.max_colwidth', None)
            return dataFrame
        else:
            vectorResult = parseEntry(entry)
            blsDF["similarity"] = blsDF["vector"].apply(lambda x: 1 - spatial.distance.cosine(x, vectorResult))
            dataFrame = blsDF.sort_values(by="similarity", ascending=False)
            dataFrame = dataFrame.head(20)
            dataFrame = dataFrame.drop(columns=["vector"])
            dataFrame = dataFrame.reset_index(drop=True)
            pd.set_option('display.max_colwidth', None)
            return dataFrame
    elif blsORNAPCS == "NAPCS":
        if searchMethod == "1":
            searchWord = str(input("What code would you like to search exactly for? "))
            searchWord = " " + searchWord + " "
            tempDF["combinedCodes"] = tempDF["Class title"] + " " + tempDF["Class definition"]
            dataFrame = tempDF[tempDF["combinedCodes"].str.contains(searchWord)]
            dataFrame = dataFrame.drop(columns=["combinedCodes","vector"])
            dataFrame = dataFrame.reset_index(drop=True)
            pd.set_option('display.max_colwidth', None)
        else:
            vectorResult = parseEntry(entry)
            tempDF["similarity"] = tempDF["vector"].apply(lambda x: 1 - spatial.distance.cosine(x, vectorResult))
            dataFrame = tempDF.sort_values(by="similarity", ascending=False)
            dataFrame = dataFrame.head(20)
            dataFrame = dataFrame.drop(columns=["vector"])
            dataFrame = dataFrame.reset_index(drop=True)
            pd.set_option('display.max_colwidth', None)
        filterLength = str(input("Would you like to filter the output based on the length of the code? (0 for No, 1 for Yes) "))
        while filterLength != "0" and filterLength != "1":
            filterLength = str(input("Would you like to filter the output based on the length of the code? "))
        if filterLength == "1":
            lengthOfCode = str(input("How long are the codes? "))
            while lengthOfCode != "3" and lengthOfCode != "5" and lengthOfCode != "6" and lengthOfCode != "7":
                lengthOfCode = str(input("How long are the codes? "))
            dataFrame = dataFrame[dataFrame.Code.str.len() == int(lengthOfCode)]
        # first digit bit
        filterFirstNum = str(input("Would you like to filter the output based on the first digit of the code? (0 for No, 1 for Yes) "))
        while filterFirstNum != "0" and filterFirstNum != "1":
            filterFirstNum = str(input("Would you like to filter the output based on the first digit of the code? "))
        if filterFirstNum == "1":
            firstDigit = str(input("What is the first digit to search for? "))
            dataFrame = dataFrame[dataFrame.Code.astype(str).str[:1] == firstDigit]
        
        return dataFrame

vectorStoragePath = vectorStoragePathCreation()
#nlp = spacy.load(os.path.expanduser("~/anaconda3/Lib/site-packages/en_core_web_lg/en_core_web_lg-2.3.1"))
nlp = spacy.load("en_core_web_lg")
blsDF = checkForBLS(vectorStoragePath)
tempDF = checkForNAPCS(vectorStoragePath)
while True:
    vectorSearchCode = ""
    searchMethod = str(input("Which method would you like to search by exact word? (1 for Yes, 0 for No):  "))
    while searchMethod != "0" and searchMethod != "1":
        searchMethod = str(input("Which method would you like to search by exact word? (1 for Yes, 0 for No):  "))  
    if searchMethod == "0":
        vectorSearchCode = str(input("Enter a string to parse? "))
    display(prototypeMatch(vectorSearchCode,searchMethod))
    compareAgain = str(input("Would you like to compare another code? (1 for Yes, 0 for No): "))
    while compareAgain != "1" and compareAgain != "0":
        compareAgain = str(input("Would you like to compare another code? (1 for Yes, 0 for No): "))
    if compareAgain == "0":
        break