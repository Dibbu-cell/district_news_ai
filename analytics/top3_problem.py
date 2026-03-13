import pandas as pd


def get_top3(df):

    counts = df["cluster"].value_counts()

    return counts.head(3)