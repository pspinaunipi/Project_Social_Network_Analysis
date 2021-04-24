"""
Simple script that searchs for crossposts to investigate the relationship
between subreddits
"""
from time import time
import concurrent.futures
import sys
import praw
import pandas as pd


def scrape_data(name, maximum,reddit):
    """
    This is the main function used to collect data. We look at the top daily posts
    and look for crosspost. Once we find one we save data from the original post
    and all its crossposts into a pandas DataFrame.

    Parameters
    ----------
    name:  str
        A string that contains the name of the subreddit from which we want to collect data.
    maximum: int
        The number of top daily posts we look in each subreddit
    reddit: praw reddit object
        The Reddit object containig the API credentials.

    Yields
    ------
    data: pd.DataFrame
        DataFrame containing informations about the crossposts and the original post such
        as number of comments, date etc
    """
    dictionary = {}
    #create empty list that will contain important information about the crossposts
    # such as number of comments, score, etc
    lst_from = [0 for _ in range(10000)]
    lst_to = [0 for _ in range(10000)]
    lst_id = [0 for _ in range(10000)]
    lst_title = [0 for _ in range(10000)]
    lst_score = [0 for _ in range(10000)]
    lst_date = [0 for _ in range(10000)]
    lst_comments = [0 for _ in range(10000)]
    lst_parents  = [0 for _ in range(10000)]
    i=0

    print("Analyzing crossposts form subreddit: {}".format(name))
    subreddit1 = reddit.subreddit('{}'.format(name))
    # look at the top posts. The number of posts considered is equal to the
    # value of the variable maximum
    current_subreddit = subreddit1.hot(limit=maximum)
    # for each of these posts look for crossposts
    for sub in current_subreddit:
        first_time = True
        for item in sub.duplicates(params={'crossposts_only': True}):
            # ignore the private users

            if str(item.subreddit)[0:2] != "u_" and item.subreddit != sub.subreddit:
                # i use a try block because bad connection error are quite common and
                try:
                    # save informations about the post
                    if first_time is True:
                        lst_from[i] = str(sub.subreddit)
                        lst_to[i] = str(sub.subreddit)
                        lst_id[i] = str(sub.id)
                        lst_title[i] = str(sub.title)
                        lst_score[i] = int(sub.score)
                        lst_date[i] = str(sub.created)
                        lst_comments[i] = int(sub.num_comments)
                        parent = reddit.submission(id=item.crosspost_parent.split("_")[1])
                        lst_parents[i]=str(parent.subreddit)
                        i=i+1

                    # informations about each crosspost
                    lst_from[i] =str(sub.subreddit)
                    lst_to[i] = str(item.subreddit)
                    lst_id[i] = str(item.id)
                    lst_title[i] = str(item.title)
                    lst_score[i] = int(item.score)
                    lst_date[i] = str(item.created)
                    lst_comments[i] = int(item.num_comments)
                    lst_parents[i] = str(parent.subreddit)
                    first_time = False
                    i=i+1

                except KeyboardInterrupt:
                    # quit
                    sys.exit()
                # if something goes wrong print an error message
                except:
                    print ("error")


    # save the lists in a dictionary
    dictionary["from"] = lst_from[:i]
    dictionary["to"] = lst_to[:i]
    dictionary["id"] = lst_id[:i]
    dictionary["title"] = lst_title[:i]
    dictionary["score"] = lst_score[:i]
    dictionary["date"] = lst_date[:i]
    dictionary["comments"] = lst_comments[:i]
    dictionary["parent"] = lst_parents[:i]
    # convert the dictionary into a DataFrame
    data = pd.DataFrame(dictionary)

    return data


if __name__ == "__main__":

    # name pf the subreddit in which the scrapring process starts
    STARTING_SUBREDDIT = "environment"
    # set to true if it is the first time you run the programm.
    NEW_START = False
    # the maximum number of post we want to check on each subreddit
    MAXIMUM = 50

    # insert your reddit credentials
    reddit = praw.Reddit(client_id='q5_B5l8wOFasRQ',
                         client_secret='q0o9lSvANudm6fxcXWb8Igo2lOFUbg',
                         user_agent='pyroblast')

    # This part of the code must be run only the first time the scraping process starts
    # otherwise all progress will be overwritten
    if NEW_START is True:

        # start looking for crossposts in the starting subreddit
        df = pd.DataFrame.from_dict(scrape_data(
            STARTING_SUBREDDIT,MAXIMUM,reddit))
        #  save the results
        df.to_csv("data/data_subreddit_2.0.csv")

    #load the data and print the number of nodes
    data_sub = pd.read_csv("data/data_subreddit_2.0.csv",index_col=0)
    print(len(pd.unique(data_sub["to"])))

    # already_scraped is a list in wich there are all the subreddit we have already
    # scraped to avoid losing time scraping them again
    to_scrape = pd.read_csv("data/to_scrape.csv",index_col=0,squeeze=True)
    # if the number of subreddit to scrape is very high run the code in parallel
    # to speed up the time to collect data
    while to_scrape.shape[0] >= 50:
        print("Subreddits to scrape:{}".format(len(to_scrape)))
        start = time()
        # create 50 parallel processes. Only 8 at times will be executed.
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # assign a different subreddit to scrape to each process
            element = [to_scrape.iat[i] for i in range(50)]
            max_scrape = [MAXIMUM for _ in range(50)]
            r = [reddit for _ in range(50)]
            #start teh parallelization
            new_datas = executor.map(scrape_data,element,max_scrape,r)
            # merge the results
            new_sub = pd.concat([new_data for new_data in new_datas])
        # print the time needed for the parallelization to complete
        finish = time()
        mins = (finish-start)//60
        secs = int((finish-start)%60)
        print("time to scrape 50 subreddits: {} min {} sec".format(mins,secs))
        # add the new data to the existing data
        data_sub = pd.concat([data_sub,new_sub])
        #drop the name of the subreddit we have scraped to the list of subreddit
        # to scrape
        to_scrape.drop(to_scrape.index[0:50],inplace=True)
        #save the results
        data_sub.to_csv("data/data_subreddit_2.0.csv")
        to_scrape.to_csv("data/to_scrape.csv")

    # once few subreddits remain start to collect data one subreddit at times
    for element in to_scrape:
        #create a dataframe with the informations about the crossposts
        new_df=pd.DataFrame.from_dict(scrape_data(element,MAXIMUM,reddit))
        # add the new data to the existing data
        data_sub = pd.concat([data_sub, new_df])
        #drop the name of the subreddit we have scraped to the list of subreddit
        # to scrape
        to_scrape.drop(to_scrape.index[0],inplace=True)
        #save the results
        data_sub.to_csv("data/data_subreddit_2.0.csv")
        to_scrape.to_csv("data/to_scrape.csv")
