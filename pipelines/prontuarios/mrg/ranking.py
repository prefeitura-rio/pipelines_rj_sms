# -*- coding: utf-8 -*-
"""
Ranking sources for data fields with conflicts
"""
import pandas as pd


def ranking_values(df_unique, field, ranks_merge):
    from_to_dict = ranks_merge[ranks_merge["campo"] == field][["smsrio", "vitai"]].to_dict(
        orient="records"
    )[0]
    df_unique.loc[:, "ranking"] = df_unique.loc[:, "system"].map(from_to_dict)

    if len(df_unique[df_unique["ranking"] == 1]) != 1:
        sub_df_r1 = df_unique[df_unique["ranking"] == 1].copy()
        sub_df_resto = df_unique[df_unique["ranking"] != 1].copy()

        sub_df_r1.sort_values(by="event_moment", ascending=False, inplace=True)
        sub_df_r1["ranking"] = list(range(1, len(sub_df_r1) + 1))

        sub_df_resto.sort_values(by="ranking", ascending=False, inplace=True)
        sub_df_resto["ranking"] = range(len(sub_df_r1) + 1, len(df_unique) + 1)

        df_unique = pd.concat([sub_df_r1, sub_df_resto])

    return df_unique

