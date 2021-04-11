"""
Simple script that searchs for crossposts to investigate the relationship
between subreddits
"""
from time import time
import praw
import pandas as pd
import numpy as np


def count_el_row(lst, string):
    """
    This function counts the number of times a string appears in a list.
    """
    count = 0
    for data in lst:
        if data == string:
            count = count+1
    return count


def print_unique(d_frame):
    """
    This function transform the horrible DataFrame obtained using scraping data
    into a more useful one.
    """
    lst = []
    # list containing all the indexes of the dataframe
    list_index = d_frame.index.to_list()
    # create a list containg all the unique elements in the dataframe
    for col in d_frame:
        unique_elements = d_frame[col].unique()
        for elem in unique_elements:
            lst.append(elem)
    # check if the indexes are included too in the list
    for elem in list_index:
        if elem not in lst:
            lst.append(elem)
    # remove duplicate values and the value nothing since it is not useful
    uniq_list = list(set(lst))
    uniq_list.remove("nothing")
    n_uniq = len(uniq_list)

    print("number of unique subreddits:")
    print(n_uniq)

    dictionary = {}
    # create an empty matrix
    matrix = np.zeros((n_uniq, n_uniq))
    # fill the matrix
    for i, name in enumerate(uniq_list):
        for j in range(n_uniq):
            if name in list_index:
                matrix[int(i)][j] = count_el_row(d_frame.loc[name, :], uniq_list[j])

    # transform the matrix into a DataFrame
    for i, element in enumerate(uniq_list):
        dictionary[element] = matrix[:, int(i)]

    data = pd.DataFrame(dictionary, index=uniq_list)

    return data


def scrape_data(names, maximum=10):
    """
    This is the main function used to collect data. It identifies all the crossposts
    and finds the subreddit in wich the crosspost is posted.

    Parameters
    ----------
    names: list of str
        A list that contains all the subreddit from wich we want to collect data
    maximum: int (default=10)
        Maximum numver of post we want to consider


    Yields
    ------
    Dictionary: dict
        A dictionary with the
    """
    dictionary = {}
    lst = []
    # iterate for each subreddit
    for name in names:

        start = time()

        lst = []
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
                    # save in a list the names of the subreddits in which the post was
                    # crossposted
                    lst.append(str(item.subreddit))

        # save the list in a dictionary
        dictionary[name] = lst
        # nice prints
        finish = time()
        total = (finish-start)/60
        print("number of crosspost in other unique subreddits:{}".format(len(set(lst))))
        print("time to complete the scraping process {:.2f} min\n".format(total))

    return dictionary


if __name__ == "__main__":

    # name pf the subreddit in which the scrapring process starts
    STARTING_SUBREDDIT = ["conspiracy"]
    # how deep should the scrapring
    LEVEL = 3
    # set to true if it is the first time you run the programm
    NEW_START = True
    # the maximum number of post we want to check on each subreddit
    MAXIMUM = 15

    # insert your reddit credentials
    reddit = praw.Reddit(client_id='',
                         client_secret='',
                         user_agent='',
                         username='',
                         password='')

    if NEW_START is True:

        # start looking for crossposts in the starting subreddit
        df = pd.DataFrame.from_dict(scrape_data(
            STARTING_SUBREDDIT, maximum=MAXIMUM), orient='index')

        # unstack, drop na and sort
        df.unstack().dropna().sort_index(level=1)
        print(df)
        #  save the results
        df.to_csv("data_subreddit.csv")

    # repeat this process a number of times equal to the value of LEVEL
    for k in range(0, LEVEL):

        print("investigate level {}".format(k))
        # read the previous results
        data_sub = pd.read_csv("data_subreddit.csv", index_col=0)
        # already_scraped is a list in wich there are all the subreddit we have already
        # scraped to avoid losing time
        already_scraped = data_sub.index.to_list()
        already_scraped.append("nothing")

        # set the indexes of the dataframe we don't want to read
        if NEW_START is True and k == 0:
            ignore_sub = []
        else:
            ignore_sub = pd.read_csv("ignore.csv", index_col=0).iloc[:, 0].to_list()

        print("loading data")
        print(data_sub)

        # iterate over each index of the dataframe that is not in the ignore_sub list
        for i, index in enumerate(data_sub.index):
            if index not in ignore_sub:
                # create a list of array we want to scrape
                analyze = data_sub.iloc[i, :].unique()
                # check if we haven't already scraped them
                for element in already_scraped:
                    if element in analyze:
                        analyze = np.delete(analyze, np.where(analyze == element))
                print("subreddit to analyze:")
                print(analyze)
                # start scraping
                if len(analyze) > 0:
                    for element in analyze:
                        dummy = []
                        dummy.append(element)
                        # create a dataframe with the result of the scraping process
                        new_df = pd.DataFrame.from_dict(scrape_data(
                            dummy, maximum=MAXIMUM), orient='index')

                        # unstack, drop na and sort
                        new_df.unstack().dropna().sort_index(level=1)

                        # concatenate new and old dataframe
                        data_sub = pd.concat([data_sub, new_df])
                        data_sub.fillna("nothing", inplace=True)
                        # save the result
                        data_sub.to_csv("data_subreddit.csv")
                        # add to the already scraped list the subreddit we have just
                        # scraped
                        already_scraped.append(element)

                    # add to the ignore_sub list the index containign all the
                    # subreddit we have just scraped
                    ignore_sub.append(index)
                    ignore = pd.Series(ignore_sub)
                    ignore.to_csv("ignore.csv")

    data_sub = pd.read_csv("data_subreddit.csv", index_col=0)
    print(data_sub)
    matrix_nn = print_unique(data_sub)
    matrix_nn.to_csv("matrix_nn.csv")
