from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "RDD Actions")

# RDD Actions

# collect()
rdd_collect = sc.parallelize([1, 2, 3])
collected_data = rdd_collect.collect()

# take()
rdd_take = sc.parallelize([1, 2, 3, 4, 5])
first_three_elements = rdd_take.take(3)

# top()
rdd_top = sc.parallelize([5, 1, 3, 2, 4])
top_three_elements = rdd_top.top(3)

# count()
rdd_count = sc.parallelize([1, 2, 3, 4, 5])
count = rdd_count.count()

# min()
rdd_min = sc.parallelize([5, 1, 3, 2, 4])
minimum = rdd_min.min()

# max()
rdd_max = sc.parallelize([5, 1, 3, 2, 4])
maximum = rdd_max.max()

# mean()
rdd_mean = sc.parallelize([1, 2, 3, 4, 5])
mean = rdd_mean.mean()

# reduce()
rdd_reduce = sc.parallelize([1, 2, 3, 4, 5])
sum_of_elements = rdd_reduce.reduce(lambda x, y: x + y)

# countByKey()
rdd_countbykey = sc.parallelize([('a', 1), ('b', 2), ('a', 3)])
count_by_key = rdd_countbykey.countByKey()

# countByValue()
rdd_countbyvalue = sc.parallelize([1, 2, 2, 3, 3, 3])
count_by_value = rdd_countbyvalue.countByValue()

# fold()
rdd_fold = sc.parallelize([1, 2, 3, 4, 5])
sum_of_elements_fold = rdd_fold.fold(0, lambda x, y: x + y)

# range()
rdd_range = sc.range(1, 10)

# Printing results
print("Collected Data:", collected_data)
print("First Three Elements:", first_three_elements)
print("Top Three Elements:", top_three_elements)
print("Count:", count)
print("Minimum:", minimum)
print("Maximum:", maximum)
print("Mean:", mean)
print("Sum of Elements (Reduce):", sum_of_elements)
print("Count by Key:", count_by_key)
print("Count by Value:", count_by_value)
print("Sum of Elements (Fold):", sum_of_elements_fold)

# Shutdown SparkContext
sc.stop()

