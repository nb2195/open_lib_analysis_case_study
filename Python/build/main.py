from pyspark.sql import SparkSession
from pyspark.sql import types as T
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime as dt
import build.utils.spark_utility as spark_util
import build.utils.flatten_json as flat_util
import datetime
import sys, yaml

class Solution():

    def __init__(self):
        self.logger = None
    
    def Harry_Potter(self,spark,filtered_inp):
        self.logger.warn(''' Initiating Harry Potter calculation ''')
        try:
            out_df = filtered_inp.where('upper(title) like "%HARRY%POTTER%" OR upper(description) like "%HARRY%POTTER%"')
            self.logger.warn(''' Harry Potter calculation complete ''')
        except Exception as e:
            self.logger.error(''' calculation error ''')
            raise ValueError("{error}\n Unable to calculate".format(error=str(e)))
        
        return out_df

    def Book_with_most_pages(self,spark,filtered_inp):
        self.logger.warn(''' Initiating Book_with_most_pages calculation ''')
        try:
            max_pg = filtered_inp.agg(max(col('number_of_pages'))).head()[0]

            out_df = filtered_inp.where('number_of_pages={0}'.format(max_pg))\
                                 .select('title','number_of_pages')\
                                 .distinct()
            self.logger.warn(''' calculation complete ''')
        except Exception as e:
            self.logger.error(''' calculation error ''')
            raise ValueError("{error}\n Unable to calculate".format(error=str(e)))
        
        return out_df

    def Top_five_authors_with_most_written_books(self,spark,filtered_inp):
        self.logger.warn(''' Initiating Top_five_authors_with_most_written_books calculation ''')
        try:
            out_df = filtered_inp.withColumn('isbn_comb',coalesce('isbn_10','isbn_13'))\
                                 .where('authors_key IS NOT NULL')\
                                 .groupBy('authors_key')\
                                 .agg(count('isbn_comb').alias('cnt_isbn_comb'))\
                                 .orderBy('cnt_isbn_comb',ascending=False)\
                                 .limit(5)\
                                 .withColumn('authors_key',split(col('authors_key'),'/').getItem(2))
            self.logger.warn(''' calculation complete ''')
        except Exception as e:
            self.logger.error(''' calculation error ''')
            raise ValueError("{error}\n Unable to calculate".format(error=str(e)))
        
        return out_df

    def Top_five_genres_with_most_books(self,spark,filtered_inp):
        self.logger.warn(''' Initiating Top_five_genres_with_most_books calculation ''')
        try:
            out_df = filtered_inp.withColumn('isbn_comb',coalesce('isbn_10','isbn_13'))\
                                .where('genres IS NOT NULL')\
                                .groupBy('genres')\
                                .agg(count(col('isbn_comb')).alias('cnt_isbn_comb'))\
                                .orderBy(col('cnt_isbn_comb'),ascending=False)\
                                .limit(5)
            self.logger.warn(''' calculation complete ''')
        except Exception as e:
            self.logger.error(''' calculation error ''')
            raise ValueError("{error}\n Unable to calculate".format(error=str(e)))
        
        return out_df

    def avg_number_of_pages(self,spark,filtered_inp):
        self.logger.warn(''' Initiating avg_number_of_pages calculation ''')
        try:
            out_df = filtered_inp.agg(round(avg('number_of_pages'),3).alias('avg_num_pages'))
            self.logger.warn(''' calculation complete ''')
        except Exception as e:
            self.logger.error(''' calculation error ''')
            raise ValueError("{error}\n Unable to calculate".format(error=str(e)))
        
        return out_df

    def num_of_authors_published_at_least_one_book_per_year(self,spark,filtered_inp):
        self.logger.warn(''' Initiating num_of_authors_published_at_least_one_book_per_year calculation ''')
        try:
            out_df = filtered_inp.withColumn('isbn_comb',coalesce('isbn_10','isbn_13'))\
                                .select(col('publish_date').cast('int'),'authors_key','isbn_comb')\
                                .groupBy('publish_date','authors_key')\
                                .agg(count('isbn_comb').alias('cnt_isbn_comb'))\
                                .where('cnt_isbn_comb > 0')\
                                .groupBy('publish_date')\
                                .agg(count('authors_key').alias('cnt_authors'))\
                                .orderBy('publish_date')
            self.logger.warn(''' calculation complete ''')
        except Exception as e:
            self.logger.error(''' calculation error ''')
            raise ValueError("{error}\n Unable to calculate".format(error=str(e)))
        
        return out_df

    def filter_df(self,spark,filtered_inp):
        try:
            self.logger.warn('''applying necessary filters to input flattened dataframe ''')
            filtered_inp = flattened_df.where('title IS NOT NULL')\
                                       .where('number_of_pages > 20')\
                                       .where('publish_date > 1950')
            self.logger.warn('''data filter successful ''')
        except Exception as e:
            self.logger.error(''' error while filtering data ''')
            raise ValueError("{error}\n Unable to filter data".format(error=str(e)))
        
        return filtered_inp
    
    def main(self, config_loc, alert_loc):
        # reading config location
        with open(config_loc) as json_file:
            config_main = yaml.load(json_file)
        # do spark submit and initialize spark
        spark, self.logger = spark_util.create_session(**config_main)

        #read the input JSON file into a spark dataframe
        self.logger.warn('''reading input JSON file ''')
        try:

            inp = spark.read.option('encoding','utf-8')\
                            .json(config_main['input_path'])
            self.logger.warn('''input read successful ''')
        except Exception as e:
            self.logger.error(''' file read error ''')
            raise ValueError("{error}\n Unable to read file".format(error=str(e)))
        #call flatten method to resolve complex data types in JSON file
        try:

            self.logger.warn('''calling method to flatten JSON ''')
            flattened_df = flat_util.flatten(inp)
            self.logger.warn('''flatten JSON successful ''')
        except Exception as e:
            self.logger.error(''' error while flattening JSON ''')
            raise ValueError("{error}\n Unable to flatten JSON file".format(error=str(e)))
        
        #applying necessary filters to input flattened dataframe
        filtered_inp = Solution.filter_df(spark,flattened_df)

        self.logger.warn('''calling metric methods''')
        Harry_Potter_df = Solution.Harry_Potter(spark,filtered_inp)
        HBook_with_most_pages_df = Solution.Book_with_most_pages(spark,filtered_inp)
        Top_five_authors_with_most_written_books_df = Solution.Top_five_authors_with_most_written_books(spark,filtered_inp)
        Top_five_genres_with_most_books_df = Solution.Top_five_genres_with_most_books(spark,filtered_inp)
        avg_number_of_pages_df = Solution.avg_number_of_pages(spark,filtered_inp)
        num_of_authors_published_at_least_one_book_per_year_df = Solution.num_of_authors_published_at_least_one_book_per_year(spark,filtered_inp)
        self.logger.warn('''metric calculation successful''')
        
        """
        Post successful metric calculations the results can eb stored in Hive tables for end users to query and access the data
        """



if __name__ == '__main__':
    config = sys.argv[1]
    obj = Solution()
    obj.main(config)
