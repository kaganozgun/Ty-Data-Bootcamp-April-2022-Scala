import reader.{readAndParseMovie, readRatings, readTags}
import task.{calculateNumberOfTagAndVote, getFirstEventTypeByGenre, getMinGenreByYears, getMostVotedUserMetrics, goldenYearOfCinema, groupByYearAndGenre}

object main {

  def main(args: Array[String]): Unit = {
    val movieList = readAndParseMovie()
    val ratingList = readRatings()
    val tagList = readTags()

    // Exercise 1-1
    val moviesGroupedByYearAndGenre = groupByYearAndGenre(movieList)
    println("Exercise 1:")
    moviesGroupedByYearAndGenre.foreach(println)

    // Exercise 1-2
    val yearAvgRating = goldenYearOfCinema(ratingList, movieList)
    println("Exercise 2:")
    yearAvgRating.sortBy(_._2).reverse.foreach(println)

    // Exercise 1-3
    val minGenreByYears = getMinGenreByYears(movieList)
    println("Exercise 3:")
    minGenreByYears.foreach(println)

    // Exercise 1-4
    val tagAndVote = calculateNumberOfTagAndVote(movieList, ratingList, tagList)
    println("Exercise 4:")
    tagAndVote.sortBy(_._1).foreach(println)

    // Exercise 1-5
    val userWithMaxVote = getMostVotedUserMetrics(tagList, movieList, ratingList)
    println("Exercise 5:")
    println(userWithMaxVote)

    //Exercise 1-6
    val fistEventByGenre = getFirstEventTypeByGenre(tagList, ratingList, movieList)
    println("Exercise 6:")
    fistEventByGenre.foreach(println)
  }

}
