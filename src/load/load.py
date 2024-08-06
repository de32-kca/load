import pandas as pd
from tabulate import tabulate


def load_data(path="~/code/de32-kca/transform_kca"):
    df=pd.read_parquet("~/code/de32-kca/transform_kca")

    for c in ["salesAmt","audiCnt","scrnCnt","showCnt"]:
        df[c]=df[c].astype(int)

    cnt=df.groupby("movieCd").count()["load_dt"]

    movieList=df[["movieCd","movieNm"]].drop_duplicates()

    cntByMovie=movieList.merge(cnt, on="movieCd").sort_values("load_dt",ascending=False)
    cntByMovie.columns=["movieCd","movieNm","movieCnt"]

    print(tabulate(cntByMovie,headers=["movieCd","movieNm","movieCnt"], tablefmt="outline"))
    #return cntByMovie
