"""

"""
from time import time
import praw
import pandas as pd
import numpy as np
import concurrent.futures
import gc
import sys

def compute_time(fin,beg,it):
    """
    simple function to compute the execution time of a task
    """
    mins = (fin-beg)//60
    secs = int((fin-beg)%60)
    params = it*8
    print("time to find {} parents: {} min {} sec".format(params,mins,secs))
    pass

def retrieve_parent(reddit,list_id):
    """
    Given a list containing ids of duplicates post, for each one of those this function
    retrieve the parent subreddit

    Parameters
    ----------
    reddit:  str
        A praw reddit object
    list_id: list of str
        List containing the id of the duplicate posts

    Yields
    ------
    series_p: pd.Series
        A series containg the parent subreddits of the duplicate posts
    """

    parents = []
    for i,id in enumerate(list_id):
        # retrieve post
        try:
            post = reddit.submission(id=id)
            #retrieve parent and delete first three element of the string
            parent = reddit.submission(id=post.crosspost_parent.split("_")[1])
            parents.append(str(parent.subreddit))
        # this is to make sure to end the programm once we use ctr+c
        except KeyboardInterrupt:
            # quit
            sys.exit()
        #if an error raises do something
        except:
            parents.append("error " + id)
            print("An error occurred")
        #convert list into pd.Series
        series_p = pd.Series(data=parents,name="parent",dtype="str")

    return series_p

def clean_data(df):
    """
    This function deletes from a dataframe redundand information. If a row contains
    an id already present in the DataFrame it will be deleted.

    Parameters
    ----------
    df:  pd.DataFrame
        Dataframe containig duplicate data

    Yields
    ------
    df: pd.DataFrame
        A new DataFrame without duplicate ids
    """

    # Remove rows wich contain id we have already analyzed
    print("Number of columns before data cleaning: {}".format(data.shape[0]))
    df.drop_duplicates(subset="id",inplace=True)
    df.dropna(inplace=True)
    print("Number of columns after data cleaning: {}".format(data.shape[0]))
    return df

if __name__=="__main__":
    #DO NOT CHANGE
    ITER = 20
    #ALWAYS SET TO FALSE
    DATA_CLEANING = False
    # the student utilizing the script there are 4 possibilities:
    # "CHIARA_M", "CHIARA_B", "CINZIA", "PAOLO"
    STUDENT = "PAOLO"

    # insert reddit credentials
    reddit = praw.Reddit(client_id='',
                         client_secret='',
                         user_agent='')

    #this part of the code removes duplicate ids
    if DATA_CLEANING is True:
        # load data and reset indexes
        data = pd.read_csv("data/data_subreddit.csv",index_col=0)
        data.reset_index(inplace=True)
        #remove duplicates
        new_data = clean_data(data)
        new_data.to_csv("data/cleaned_data_subreddit.csv")
        del (data,new_data)
        gc.collect()


    #load ids to analyze
    data = pd.read_csv("data/ids_{}.csv".format(STUDENT),index_col=0,squeeze=True)
    # load series wich contain the parent subreddits name
    series_parent= pd.read_csv("data/parents_{}.csv".format(STUDENT),index_col=0,squeeze=True)
    first_index = series_parent.shape[0]
    print("IDs to analyze: {}".format(len(data)))
    print ("parents to retrieve: {}".format(data.shape[0] - first_index))

    while first_index < 3.10e4:
        #look at 160 ids at every iteration
        ids = data.iloc[first_index:]
        start = time()
        # Used multiprocessing to speed-up computational time
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # each process looks at 20 datas at every iteration
            id = [ids.iloc[ITER*j:ITER*(j+1)] for j in range(8)]
            r = [reddit for _ in range(8)]
            # save the results for the processes
            results = executor.map(retrieve_parent,r,id)
            # merge the results
            new_parent = pd.concat([result for result in results],ignore_index=True)
            series_parent = pd.concat([series_parent,new_parent],ignore_index=True)
            # save them in a Series
            series_parent.to_csv("data/parents_{}.csv".format(STUDENT))
        #print computational time
        finish = time()
        compute_time(finish,start,ITER)
        #look at the next 160 indexes
        first_index=first_index+ITER*8

    # for the last subreddits in the Series retrieve the parents one by one
    # without multiprocessing
    ids = data.iloc[first_index:]
    new_parent=retrieve_parent(reddit,ids)
    series_parent = pd.concat([series_parent,new_parent],ignore_index=True)
    series_parent.to_csv("data/parents_{}.csv".format(STUDENT))
    print("DONE :)")
