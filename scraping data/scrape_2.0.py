"""
Simple script that searchs for crossposts to investigate the relationship
between subreddits
"""
from time import time
import praw
import pandas as pd
import numpy as np


def create_list_sub():
    """
    This function creates a list of all the element we must scrape.
    """
    already_scraped = pd.unique(data_sub["from"])
    to_scrape = pd.unique(data_sub["to"])
    #delete subreddits we have alredy scraped
    for element in to_scrape:
        if element in already_scraped:
            to_scrape = np.delete(to_scrape, np.where(to_scrape == element))

    return to_scrape

def scrape_data(name, maximum=10):
    """
    This is the main function used to collect data. It identifies all the crossposts
    and finds the subreddit in wich the crosspost is posted.

    Parameters
    ----------
    name:  str
        A string that contains the name of the subreddit from which we want to collect data
    maximum: int (default=10)
        Maximum numver of post we want to consider


    Yields
    ------
    Dictionary: dict
        A dictionary with the
    """
    start = time()
    dictionary = {}
    #create empty list that will contain important information about the crossposts
    # such as number of comments, score, etc
    lst_from = []
    lst_to = []
    lst_id = []
    lst_title = []
    lst_score = []
    lst_date = []
    lst_comments = []

    print("Analyzing crossposts form subreddit: {}".format(name))
    subreddit1 = reddit.subreddit('{}'.format(name))
    # look at the top posts. The number of posts considered is equal to the
    # value of the variable maximum
    current_subreddit = subreddit1.hot(limit=maximum)
    # for each of these posts look for crossposts
    for sub in current_subreddit:

        for item in sub.duplicates(params={'crossposts_only': True}):
            # ignore the private users
            if str(item.subreddit)[0:2] != "u_":
                # informations about the crosspost
                lst_from.append(str(sub.subreddit))
                lst_to.append(str(item.subreddit))
                lst_id.append(str(item.id))
                lst_title.append(str(item.title))
                lst_score.append(int(item.score))
                lst_date.append(str(item.created))
                lst_comments.append(int(item.num_comments))

    # save the lists in a dictionary
    dictionary["from"] = lst_from
    dictionary["to"] = lst_to
    dictionary["id"] = lst_id
    dictionary["title"] = lst_title
    dictionary["score"] = lst_score
    dictionary["date"] = lst_date
    dictionary["comments"] = lst_comments
    # nice prints
    finish = time()
    total = (finish-start)/60
    print("number of crosspost in other unique subreddits:{}".format(len(set(lst_to))))
    print("time to complete the scraping process {:.2f} min\n".format(total))

    return dictionary


if __name__ == "__main__":

    # name pf the subreddit in which the scrapring process starts
    STARTING_SUBREDDIT = "environment"
    # set to true if it is the first time you run the programm.
    # ALWAYS SET TO FALSE
    NEW_START = False
    # the maximum number of post we want to check on each subreddit
    MAXIMUM = 40
    # the student utilizing the script there are 4 possibilities:
    # "CHIARA_M", "CHIARA_B", "CINZIA", "PAOLO"
    STUDENT = "PAOLO"
    # ALWAYS SET TO FALSE
    CREATE_LIST = False
    # ALWAYS SET TO TRUE
    SCRAPE = False
    #ALWAYS SET TO FALSE
    NEW_LEVEL = False
    # insert your reddit credentials
    reddit = praw.Reddit(client_id='',
                         client_secret='',
                         user_agent='')

    # This part of the code must be run only the first time the scraping process starts
    # otherwise all progress will be overwritten
    if NEW_START is True:

        # start looking for crossposts in the starting subreddit
        df = pd.DataFrame.from_dict(scrape_data(
            STARTING_SUBREDDIT, maximum=MAXIMUM))

        #  save the results
        df.to_csv("data_subreddit_2.0.csv")

    data_sub = pd.read_csv("data_subreddit_2.0.csv",index_col=0)

    # this part of the code is foundamental for parallelization. We create 4 lists
    # of subreddit to scrape, each list has a different subreddits to avoid scraping
    # the same subreddit twice. Then each one of us scrape all the subreddits in
    # a different list

    if CREATE_LIST is True:
        #create a list with all the subreddit to scrape
        to_scrape = create_list_sub()
        # divide this list into 4 parts
        individual_len=len(to_scrape)//4
        to_scrape_c_m =pd.Series(to_scrape[0 : individual_len])
        to_scrape_c_b =pd.Series(to_scrape[individual_len : 2*individual_len])
        to_scrape_cin =pd.Series(to_scrape[2*individual_len : 3*individual_len])
        to_scrape_p =pd.Series(to_scrape[3*individual_len :])
        #save those 4 parts
        to_scrape_c_m.to_csv("to_scrape_CHIARA_M.csv")
        to_scrape_c_b.to_csv("to_scrape_CHIARA_B.csv")
        to_scrape_cin.to_csv("to_scrape_CINZIA.csv")
        to_scrape_p.to_csv("to_scrape_PAOLO.csv")

    #this is the main module of this script. It retrieves information about
    # crossposts and saves them into a DataFrame
    if SCRAPE is True:
        # already_scraped is a list in wich there are all the subreddit we have already
        # scraped to avoid losing time scraping them again
        to_scrape = pd.read_csv("to_scrape_{}.csv".format(STUDENT),index_col=0,squeeze=True)
        print("Subreddits to scrape:{}".format(len(to_scrape)))

        for element in to_scrape:
            #create a dataframe with the informations about the crossposts
            new_df=pd.DataFrame.from_dict(scrape_data(element, maximum=MAXIMUM))
            # "append" to the dataframe we already have
            data_sub = pd.concat([data_sub, new_df])
            #drop the name of the subreddit we have scraped to the list of subreddit
            # to scrape
            to_scrape.drop(to_scrape.index[0],inplace=True)
            #save the results
            data_sub.to_csv("data_subreddit_2.0.csv")
            to_scrape.to_csv("to_scrape_{}.csv".format(STUDENT))

    # this part of the code merges the data from the scraping process
    if NEW_LEVEL is True:
        #import data
        df1=pd.read_csv("data_subreddit_PAOLO.csv",index_col=0)
        df2=pd.read_csv("data_subreddit_CINZIA.csv",index_col=0)
        df3=pd.read_csv("data_subreddit_CHIARA_M.csv",index_col=0)
        df4=pd.read_csv("data_subreddit_CHIARA_B.csv",index_col=0)
        #merge them and drop duplicate rows
        df=pd.concat((df1,df2,df3,df4))
        df.drop_duplicates(inplace=True)
        #save new data
        df.to_csv("data_subreddit_2.0.csv")
        #print the numbers of unique nodes
        print(len(pd.unique(df["to"])))
