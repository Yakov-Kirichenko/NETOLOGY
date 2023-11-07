from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lead,round,max
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("COVID-19 Data Analysis").getOrCreate()
df=(spark.read.option('header',True).option('inferSchema',True).csv('/home/owid-covid-data.csv'))
df1=df.select(df.iso_code,df.location,(round(((df.total_cases*100) / df.population),0)).alias('perscent'))\
.filter(df.date=='2021-03-31')\
.orderBy(((df.total_cases*100) / df.population).desc()).limit(15)
df1.write.csv('/home/covid-result1.csv')
df_tr=df.select('date','location','new_cases')\
.where(~col('location').isin('Asia','Europe','European Union','World','South America','North America'))\
.where(col('date').between('2021-03-22','2021-03-28')).groupBy('location').agg(max('new_cases').alias('new_cases')).orderBy(col('new_cases').desc()).limit(10)
df_result=df_tr.join(df,['location','new_cases']).select('date','location','new_cases').orderBy(col('new_cases').desc())
df_result.write.csv('/home/covid-result2.csv')
windowSpec  = Window.partitionBy().orderBy('date')
df_trws=df.select('date','location','new_cases')\
.where(df.location=='Russia')\
.withColumn('yesterday',lead('date',1).over(windowSpec))\
.where(col('date').between('2021-03-21','2021-03-28'))
stage=df_trws.select(df_trws.yesterday,df_trws.new_cases.alias('yesterday_cases'))
df_Rl=stage.join(df_trws,df_trws.date==stage.yesterday).select('date','new_cases','yesterday_cases',(col('new_cases')-col('yesterday_cases')).alias('delta'))
df_Rl.write.csv('/home/covid-result3.csv')
spark.stop
