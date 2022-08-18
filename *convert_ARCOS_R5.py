import tabula as tb
import pandas as pd
import re
import os

pdf_loc="reports/"#location of pdfs
dest="00-21*/"#folder where .csvs will be saved
file_names=list(range(2004,2005))#years you want (+1 to second number)
file_names=(str(y) for y in file_names)

for file_name in file_names:
    #set column boundaries, 2000-2005 are inconsistent when not manually set
    cols=[]
    if file_name in ["2000","2001","2003"]: cols=[242,352,436,540]
    elif file_name in ["2004"]: cols=[240,350,405,505]
    elif file_name in ["2002","2005"]: cols=[190,300,370,450]

    #convert pdf to .csv (read_pdf is just worse for some reason)
    tb.convert_into(pdf_loc+file_name+".pdf",dest+file_name+".csv","csv",stream=True,guess=False,columns=cols,pages="all")
    #fill in empty columns so pandas doesn't get mad about inconsistent row lengths
    with open(dest+file_name+".csv", 'r') as new:
        col_count = [len(l.split(",")) for l in new.readlines()]
    column_names = [i for i in range(0, max(col_count))]

    #read the file as a dataframe
    df = pd.read_csv(dest+file_name+".csv",sep=",",header=None,names=column_names)

    #find the start point (Report 5) from a list of possibilities (just trial and error, all the pdfs are slightly different...)
    for x in ["2 - REPORT  5","REPORT  5","OS 2 - REPORT  5","EPORT  5","S 2 - REPORT  5","COS 2 - REPORT","ARCOS 3 - REPORT 5","STATISTICAL SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WT","STATISTICAL SUMMARY FOR RETAIL DRUG PURCHASES"]:
        start=df.loc[df.isin([x]).any(axis=1)].index.tolist()
        if start!=[]:
            break
    #find the end point (start of Report 7) in a list of possibilities (just trial and error, all the pdfs are slightly different...)
    for x in ["2 - REPORT  7","EPORT  7","OS 2 - REPORT  7","ARCOS 2 - REPO","UNITED STATES SUMMARY FOR RETAIL DRUG PURCHASES BY GRAMS WT","UNITED STATES SUMMARY FOR RETAIL DRUG PURCHASES"]:
        end=df.loc[df.isin([x]).any(axis=1)].index.tolist()
        if end!=[]:
            break

    #check if the report is one document or all reports combined
    one_doc=False if end==[] else True
    df.to_csv("test.csv")
    #get only the chunk that you want (Report 5)
    if one_doc: df=df.iloc[start[0]:end[0],]
    else: df=df.iloc[start[0]:]

    #loop through rows and extract values
    lines=[]
    state=bus=[]
    for index,row in df.iterrows():
        row=row.astype("string")#squish everything into one string because column grouping is inconsistent

        s=row.str.cat(sep=" ")#the string separated by whitespace
        vals=s.split(" ")#if its a row with values, parse them out

        #set the current state when the loop finds it
        skip=False
        if "STATE:" in s:
            state=re.findall("(?<=STATE:).+?(?=BUS?|B U?)",s)
            if state==[]:
                state=re.findall("(?<=STATE:).+?(?=SS BUS|SS ACT)",s)#they changed the naming conventions sometime because why not
            if "-" in s and "ACTIVITY:" not in s:
                state=re.findall("(?<=- ).*",s)

        s=row.str.cat(sep="").replace(" ","")#the string without any whitespace
        #set the current bus act when the loop finds it
        if "ACTIVITY:" in s:
            bus=re.findall("(?<=SSACTIVITY:....).*",s)
            if bus==[]:
                bus=re.findall("(?<=SSACTIVITY:).*",s)#2000-2005
        #clean out lines we don't want and append new data (again, trial and error to determine what to clean out)
        elif(not any (x in vals for x in ["DRUG","DRU","DR","ARCOS 3 - REPORT","PERIOD:","GRAMSAVERAGE","AVERAGE","RAGE","--------------","-------------","---------","MENT","OTAL","WEIGHT","JUSTICE","JUSTI","REPORT","EPORT","ORT","TO","PER","AM","PM","-"]) and state!=[] and len(vals)>=5):
            for x in ["HOSPITALS","PHARMACIES","PRACTITIONERS","MID-LEVEL PRACTITIONERS","TEACHING INSTITUTIONS","NARCOTIC TREATMENT PROGRAMS"]:
                if bus[0].replace(" ","") in x.replace(" ",""):
                    bus[0]=x
                    break
            lines.append([state[0].strip(" "),bus[0],' '.join(vals[:-4]),vals[-4],vals[-3],vals[-2],vals[-1]])

    #make the final dataframe
    extract=pd.DataFrame(lines,columns=["STATE","BUS_TYPE","DRUG_NAME","DRUG_CODE","BUYERS","TOTAL_GRAMS","AVG_GRAMS"])
    #make sure numbers are the right type
    extract[["BUYERS","TOTAL_GRAMS","AVG_GRAMS"]]=extract[["BUYERS","TOTAL_GRAMS","AVG_GRAMS"]].replace({',':''},regex=True).apply(pd.to_numeric,1)

    #group and save
    year=extract.groupby(["STATE","BUS_TYPE","DRUG_NAME","DRUG_CODE"])[["BUYERS","TOTAL_GRAMS"]].apply(lambda x : x.sum())
    year.to_csv(dest+file_name+".csv")
    year_summary=extract.groupby(["BUS_TYPE","DRUG_NAME"])[["BUYERS","TOTAL_GRAMS"]].apply(lambda x : x.sum())
    year_summary.to_csv(dest+file_name+"_US.csv")
