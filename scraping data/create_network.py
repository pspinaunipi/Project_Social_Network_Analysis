import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import gc

def remove_useless_data(df):
    df_1 = df[df["parent"] == df["to"]]
    # delete duplicate rows from dataframe
    new_df = df.drop(df_1.index)
    #delete useless data to free memory
    del df,df_1
    gc.collect()
    #reset the indexes in the new DataFrame
    new_df.reset_index(inplace=True,drop=True)
    return new_df

if __name__=="__main__":

    data = pd.read_csv("data/data_subreddit_cleaned.csv",index_col=0,nrows=1000)
    no_duplicates = remove_useless_data(data)
    G = nx.convert_matrix.from_pandas_edgelist(no_duplicates,source = "parent",target="to",
                                                            edge_attr=True)
    nx.draw(G)
    plt.show()
