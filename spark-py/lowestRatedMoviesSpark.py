from pyspark import SparkConf, SparkContext


def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))


def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
            return movieNames


if __name__ == "__main__":
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf=conf)

    # Load raw data
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert to (movieId, (ratings, 1.0))
    movieRatings = lines.map(parseInput)

    # Reduce to (movieId, (sumOfRatings, totalRatingsCount))
    ratingsTotalAndCount = movieRatings.reduceByKey(
        lambda movie1, movie2: movie1[0] + movie2[0]
    )

    # Map to (movieId, avgRating)
    averageRatings = ratingsTotalAndCount.mapValues(
        lambda totalAndCount: totalAndCount[0] / totalAndCount[1]
    )

    # Sort By AverageRating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)
