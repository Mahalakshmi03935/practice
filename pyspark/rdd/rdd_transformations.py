from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "RDD Transformations")

# RDD Transformations

# map()
rdd_map = sc.parallelize([1, 2, 3, 4, 5])
mapped_rdd = rdd_map.map(lambda x: x * 2)

# flatMap()
rdd_flatmap = sc.parallelize([1, 2, 3])
flat_mapped_rdd = rdd_flatmap.flatMap(lambda x: (x, x * 2))

# filter()
rdd_filter = sc.parallelize([1, 2, 3, 4, 5])
filtered_rdd = rdd_filter.filter(lambda x: x % 2 == 0)

# mapPartitions()
rdd_partitions = sc.parallelize([1, 2, 3, 4, 5], 2)
def map_partitions_func(iterator):
    yield sum(iterator)
mapped_partitions_rdd = rdd_partitions.mapPartitions(map_partitions_func)

# mapPartitionsWithIndex()
def map_partitions_with_index_func(partition_index, iterator):
    yield partition_index, sum(iterator)
mapped_partitions_with_index_rdd = rdd_partitions.mapPartitionsWithIndex(map_partitions_with_index_func)

# glom()
glom_rdd = rdd_partitions.glom().collect()

# Union()
rdd_union1 = sc.parallelize([1, 2, 3])
rdd_union2 = sc.parallelize([4, 5, 6])
union_rdd = rdd_union1.union(rdd_union2)

# intersection()
rdd_intersection1 = sc.parallelize([1, 2, 3])
rdd_intersection2 = sc.parallelize([3, 4, 5])
intersection_rdd = rdd_intersection1.intersection(rdd_intersection2)

# distinct()
rdd_distinct = sc.parallelize([1, 2, 2, 3, 3, 4])
distinct_rdd = rdd_distinct.distinct()

# groupByKey()
rdd_groupbykey = sc.parallelize([('a', 1), ('b', 2), ('a', 3)])
grouped_rdd = rdd_groupbykey.groupByKey()

# Printing results
print("Mapped RDD:", mapped_rdd.collect())
print("Flat Mapped RDD:", flat_mapped_rdd.collect())
print("Filtered RDD:", filtered_rdd.collect())
print("Mapped Partitions RDD:", mapped_partitions_rdd.collect())
print("Mapped Partitions With Index RDD:", mapped_partitions_with_index_rdd.collect())
print("Glom RDD:", glom_rdd)
print("Union RDD:", union_rdd.collect())
print("Intersection RDD:", intersection_rdd.collect())
print("Distinct RDD:", distinct_rdd.collect())
print("Grouped RDD:", [(k, list(v)) for k, v in grouped_rdd.collect()])

# Shutdown SparkContext
sc.stop()
