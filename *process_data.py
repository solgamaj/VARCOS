import pandas as pd

buyers_list=[]
grams_list=[]

#drugs you want
drugs=["OXYCODONE"]
#export file name
export="OXY.csv"

#loop through each year
for year in [str(x) for x in range(2000,2022)]:

    #read in the file and double everything to be grouped for the total of all business types
    input=pd.read_csv("00-21/"+year+".csv")
    all=input.copy(deep=True)
    all["BUS_TYPE"]=["COMBINED"]*len(input.index)
    input=pd.concat([input,all])

    #isolate the drugs you want and group sum by business type
    buyers=input.loc[input["DRUG_NAME"].isin(drugs)].groupby(["BUS_TYPE","STATE"])[["BUYERS"]].apply(lambda x : x.sum())
    grams=input.loc[input["DRUG_NAME"].isin(drugs)].groupby(["BUS_TYPE","STATE"])[["TOTAL_GRAMS"]].apply(lambda x : x.sum())
    buyers_list+=[buyers.rename(columns={"BUYERS":year+" BUYERS"})]
    grams_list+=[grams.rename(columns={"TOTAL_GRAMS":year+" TOTAL_GRAMS"})]

#put together the weights and number of buyers
final=pd.concat(grams_list+buyers_list,axis=1).groupby(["BUS_TYPE","STATE"]).sum()

#move the territories to the bottom
idx=final.index.get_level_values("STATE").unique().to_list()
for x in ["PUERTO RICO","AMERICAN SAMOA","GUAM","VIRGIN ISLANDS"]:
    try:
        temp=idx.pop(idx.index(x))
        idx=idx+[temp]
    except:
        continue
final=final.reindex(pd.MultiIndex.from_product([final.index.get_level_values("BUS_TYPE").unique(),pd.Index(idx)], names=["BUS_TYPE","STATE"]),fill_value=0)

#change any state names that differ between years
for x in final.index.get_level_values("BUS_TYPE").unique():
    try:
        final.loc[x,"GUAM"]= final.loc[x,"GUAM"]+final.loc[x,"TRUST TERRITORIES (GUA M)"]
        final.drop((x,"TRUST TERRITORIES (GUA M)"),inplace=True)
    except:
        continue
final.to_csv(export)
