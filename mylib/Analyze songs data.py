# Databricks notebook source
# MAGIC %pip install tabulate
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which artists released the most songs each year?
# MAGIC SELECT
# MAGIC   artist_name,
# MAGIC   count(artist_name)
# MAGIC AS
# MAGIC   num_songs,
# MAGIC   year
# MAGIC FROM
# MAGIC   prepare_songs_data
# MAGIC WHERE
# MAGIC   year > 0
# MAGIC GROUP BY
# MAGIC   artist_name,
# MAGIC   year
# MAGIC ORDER BY
# MAGIC   num_songs DESC,
# MAGIC   year DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  -- Find songs for your DJ list
# MAGIC  SELECT
# MAGIC    artist_name,
# MAGIC    title,
# MAGIC    tempo
# MAGIC  FROM
# MAGIC    prepare_songs_data
# MAGIC  WHERE
# MAGIC    time_signature = 4
# MAGIC    AND
# MAGIC    tempo between 100 and 140
# MAGIC    ORDER BY tempo DESC;

# COMMAND ----------

# Spark SQL Query: Which artists released the most songs each year?
top_artists = spark.sql(
    """
    SELECT
    artist_name,
    count(artist_name)
    AS
    num_songs,
    year
    FROM
    prepare_songs_data
    WHERE
    year > 0
    GROUP BY
    artist_name,
    year
    ORDER BY
    num_songs DESC,
    year DESC
    LIMIT 10
"""
)

# COMMAND ----------

# Spark SQL Query: Find songs for your DJ list
top_DJ = spark.sql(
    """
    SELECT
    artist_name,
    title,
    tempo
    FROM
    prepare_songs_data
    WHERE
    time_signature = 4
    AND
    tempo between 100 and 140
    ORDER BY tempo DESC
    LIMIT 10
"""
)

# COMMAND ----------


from tabulate import tabulate

# Convert top_artists DataFrame to Pandas DataFrame
top_artists_pandas = top_artists.toPandas()

# Convert top_DJ DataFrame to Pandas DataFrame
top_DJ_pandas = top_DJ.toPandas()

# Convert Pandas DataFrame to Markdown format
top_artists_md = tabulate(top_artists_pandas, tablefmt="pipe", headers="keys")
top_DJ_md = tabulate(top_DJ_pandas, tablefmt="pipe", headers="keys")

# Write to result.md
with open("result.md", "w") as f:
    f.write("# Top artists\n")
    f.write(top_artists_md)
    f.write("\n\n# Top DJs\n")
    f.write(top_DJ_md)
