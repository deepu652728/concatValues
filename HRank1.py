from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    print("Session Started")

spark = SparkSession.builder.appName("HRank").master("local[*]") \
    .getOrCreate()

print("Session Created")

df1 = spark.read.csv("G:/streamingfiles/Data.csv", inferSchema=True, header=True)
print(df1.show())
df2 = df1.select('Country', f.split('Values', ';').alias('Values'))

df_sizes = df2.select(f.size('Values').alias('Values'))
df_max = df_sizes.agg(f.max('Values'))

nb_columns = df_max.collect()[0][0]


df_result = df2.select('Country', *[df2['Values'][i] for i in range(nb_columns)])


final_df = df_result.groupBy("country").agg(f.sum("Values[0]").alias("col1"), f.sum("Values[1]").alias("col2"),
                                            f.sum("Values[2]").alias("col3"), f.sum("Values[3]").alias("col4"),
                                            f.sum("Values[4]").alias("col5"))


result_df = final_df.withColumn('Values', f.concat(f.col('col1').cast(IntegerType()), f.lit(';'),
                                                   f.col('col2').cast(IntegerType()), f.lit(';'),
                                                   f.col('col3').cast(IntegerType()), f.lit(';'),
                                                   f.col('col4').cast(IntegerType()),
                                                   f.lit(';'), f.col('col5').cast(IntegerType())))


output_df = result_df.select("country", "Values")

print(output_df.show())
