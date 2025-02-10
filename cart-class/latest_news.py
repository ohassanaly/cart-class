import pandas as pd 
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number,  to_date, date_add, lit

def get_last_df(spark_df, column, default_date = "2024-12-31") :
    
    window_spec = Window.partitionBy("person_id").orderBy(col(column).desc())
    spark_df = spark_df.filter(col(column) <= default_date)
    return((
        spark_df
        .withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
    ).toPandas())

def get_first_df(spark_df, column, default_date = "2024-12-31") :
    
    window_spec = Window.partitionBy("person_id").orderBy(col(column).asc())
    spark_df = spark_df.filter(col(column) <= default_date)
    return((
        spark_df
        .withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
    ).toPandas())

def get_latest_news(person_df, last_visit_df, last_note_df, last_bio_df):
    person_df = person_df.merge(last_visit_df, on='person_id', how='left').merge(last_bio_df, on='person_id', how='left').merge(last_note_df, on = 'person_id', how='left')
    person_df['latest_news'] = person_df[['visit_end_datetime', 'note_datetime', 'measurement_datetime', "death_datetime"]].apply(lambda x: pd.to_datetime(x)).max(axis=1, skipna=True)
    return person_df

def get_follow_df(person_df) : #person_df est un pandas df
    person_df = get_latest_news(person_df, get_last_df(visit_df, "visit_end_datetime"), get_last_df(note_df, "note_datetime"), get_last_df(bio_df, "measurement_datetime"))
    
    first_visit_df = get_first_df(visit_df, "visit_start_datetime")
    first_visit_df.rename(columns = {"visit_start_datetime" : "T0", "visit_occurrence_id" : "first_visit_occurrence_id"}, inplace=True)
    person_df = person_df.merge(first_visit_df[["person_id", "first_visit_occurrence_id", "T0"]],on = "person_id", how = "left")
    
    person_df["followup_time_in_days"] = (person_df['latest_news'].dt.date - person_df['T0'].dt.date).dt.days
    
    return(person_df)