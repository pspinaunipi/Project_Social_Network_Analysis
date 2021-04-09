"""
Simple script that searchs for crossposts to investigate the relationship
between subreddits
"""
import praw
import pandas as pd

if __name__ == "__main__":
    # insert your reddit credentials
    reddit = praw.Reddit(client_id='',
                         client_secret='',
                         user_agent='',
                         username='',
                         password='')

    dict = {}
    # list containing the names of all the subreddit we want to analyze
    name_subreddit = ["conspiracy", "socialism", "Libertarian"]

    # iterate for each subreddit
    for name in name_subreddit:

        list = []
        subreddit1 = reddit.subreddit('{}'.format(name))
        # look at the top 250 posts
        current_subreddit = subreddit1.hot(limit=250)
        # for each of these posts look for crossposts
        for i, sub in enumerate(current_subreddit):
            for item in sub.duplicates(params={'crossposts_only': True}):
                # ignore the private users
                if str(item.subreddit)[0:2] != "u_":
                    # save in a list the names of the subreddits in which the post was
                    # crossposted
                    list.append(str(item.subreddit))
                    print(i, str(item.subreddit))

        # save the list in a dictionary
        dict[name] = list

    # create a DataFrame from the dict
    df = pd.DataFrame.from_dict(dict, orient='index')

    # unstack, drop na and sort
    df.unstack().dropna().sort_index(level=1)
    print(df)
    df.to_csv("example.csv")
    for index in df.index:
        print("number of unique subreddits :")
        print(len(df.loc[index, :].unique()))
