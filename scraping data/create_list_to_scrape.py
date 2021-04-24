"""
This script create and saves a list of subreddit we need to scrape. Run this function
once all the subreddit in a level are already scraped, to continue the scraping
process to the next level.
"""


import pandas as pd
import numpy as np


def create_list_sub(data):
    """
    This function creates a list of all the element we must scrape.

    Parameters
    ----------
    data:  pd.DataFrame
        Dataframe containing all the data

    Yields
    ------
    to_scrape: list
        List containig the name of the subreddits to scrape.
    """
    already_scraped = pd.unique(data["from"])
    scrape = pd.unique(data["to"])
    #delete subreddits we have alredy scraped
    for element in scrape:
        if element in already_scraped:
            # if an element is in from it means we have already scraped it
            # hence we can delete it from the list
            scrape = np.delete(scrape, np.where(scrape == element))

    return scrape

if __name__=="__main__":
    #load data
    data_sub = pd.read_csv("data/data_subreddit_2.0.csv",index_col=0)
    #create a list contining the name of the subreddit to scrape and save it
    to_scrape = create_list_sub(data_sub)
    to_scrape =pd.Series(to_scrape)
    to_scrape.to_csv("data/to_scrape.csv")
