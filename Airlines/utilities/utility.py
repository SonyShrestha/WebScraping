"""
    Author          :   Siddhi
    Created_date    :   2019/08/20  
    Modified Date   :   2019/09/26
    Description     :   Program utility function.       
"""
import pandas as pd


def read_table_sql(query, *args, **kwargs):
    """
        Read table from either query or table name
    """
    try:
        if len(query.split()) > 1:
            return pd.read_sql_query(query, *args, **kwargs)
        else:
            return pd.read_sql_table(query, *args, **kwargs)
    except Exception as e:
        raise e


def insert_df(df, table_name, if_exists='append', *args, **kwargs):
    try:
        df.to_sql(table_name, if_exists=if_exists,
                  index=False * args, **kwargs)
    except Exception as e:
        raise e
