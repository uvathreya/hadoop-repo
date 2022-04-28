from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


def parseInput(line):
    fields = line.split()
    return Row(movieID=int(fields[1]), rating=float(fields[2]))


def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
            return movieNames


if __name__ == "__main__":
    conf = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up movie ID -> name directory
    movieNames = loadMovieNames()

    # Load raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Create an RDD of (movieId, ratings)
    movies = lines.map(parseInput)

    # Create a dataframe from RDD
    movieDataset = spark.createDataFrame(movies)

    # compute avgRating by movieID
    averageRatings = movieDataset.groupBy("movies").avg("ratings")

    # Calculate the total ratings by movieID
    counts = movieDataset.groupBy("movies").count()

    # joined Dataframe of Averages and counts
    averagesAndCount = counts.join(averageRatings, "movieID")

    # Pull top 10 results
    topTen = averagesAndCount.orderBy("avg(rating)").take(10)

    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    # stop the session
    spark.stop()