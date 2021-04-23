from time import time
import praw
import pandas as pd
import numpy as np



if __name__=="__main__":
    #import data
    data = pd.read_csv("data/cleaned_data_subreddit.csv",index_col=0)
    #create series with all the ids
    ids = data["id"]


    # divide this list into 4 parts
    individual_len=ids.shape[0]//4
    ids_c_m =ids.iloc[0 : individual_len].reset_index(drop=True)
    ids_c_b =ids.iloc[individual_len : 2*individual_len].reset_index(drop=True)
    ids_cin =ids.iloc[2*individual_len : 3*individual_len].reset_index(drop=True)
    ids_p =ids.iloc[3*individual_len :].reset_index(drop=True)
    #save those 4 parts
    ids_c_m.to_csv("data/ids_CHIARA_M.csv")
    ids_c_b.to_csv("data/ids_CHIARA_B.csv")
    ids_cin.to_csv("data/ids_CINZIA.csv")
    ids_p.to_csv("data/ids_PAOLO.csv")
