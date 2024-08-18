from pyspark.sql.functions import col
class Transform:

    # TODO 3 - Add a constructor here
    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, train,meal,center):
        print("Transforming")
        # drop all the rows having null values
        train_new = train.dropDuplicates()
        train_no_nulls = train_new.dropna()
        meal = meal.dropDuplicates()
        center = center.dropDuplicates()
        result = train_no_nulls.join(meal, on="meal_id", how="inner")
        result = result.join(center,on="center_id",how="inner")
        return result