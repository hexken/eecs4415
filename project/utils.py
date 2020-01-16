"""
utils.py
--------
This file contains utility functions used in the proof of concept program for the project
'Computing TF-IDF Vectors for Subreddits', by
Ken Tjhia <hexken@my.yorku.ca>
Qijin Xu <jackxu@my.yorku.ca>
Ibrahim Suedan <isuedan@hotmail.com>
"""


def coordinateMatrixMultiply(leftmatrix, rightmatrix):
    """
    :param leftmatrix: CoordinateMatrix
    :param rightmatrix: CoordinateMatrix
    :return: PipelineRDD of the (row, col, val) tuples

    This function computes a matrix product using RDD's,
    BlockMatrix has a distributed matrix multiply however it was giving
    out of memory errors even after allocating all of my memory to the driver (single node).

    BY Stefan_Fairphone
    https://stackoverflow.com/questions/45881580/pyspark-rdd-sparse-matrix-multiplication-from-scala-to-python,
    which is a python implementation of the approach found at
    https://medium.com/balabit-unsupervised/scalable-sparse-matrix-multiplication-in-apache-spark-c79e9ffc0703
    """
    left = leftmatrix.entries.map(lambda e: (e.j, (e.i, e.value)))
    right = rightmatrix.entries.map(lambda e: (e.i, (e.j, e.value)))
    productEntries = left \
        .join(right) \
        .map(lambda e: ((e[1][0][0], e[1][1][0]), (e[1][0][1] * e[1][1][1]))) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda e: (*e[0], e[1]))
    return productEntries
