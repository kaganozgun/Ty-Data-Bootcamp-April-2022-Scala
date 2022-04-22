import model.{Movie, Rating, Tag}

import scala.io.Source
import scala.util.Try

object reader {

  def parseMovie(line: String): Movie = {
    val splitted = line.split(",", 2)

    val id = splitted(0).toInt
    val remaining = splitted(1)
    val sp = remaining.lastIndexOf(",")
    val titleDirty = splitted(1)
    val title =
      if (titleDirty.startsWith("\"")) titleDirty.drop(1) else titleDirty

    val year = Try(
      title
        .substring(title.lastIndexOf("("), title.lastIndexOf(")"))
        .drop(1)
        .toInt
    ).toOption
    val genres = remaining.substring(sp + 1).split('|').toList
    Movie(id, title, year, genres)
  }

  def readAndParseMovie(): List[Movie] ={
    val movieLines = Source.fromFile("data/movies.csv").getLines().toList.drop(1)
    val movies = movieLines.map(parseMovie)
    movies
  }

  def parseRating(line: String): Rating = {
    val splitted = line.split(",")
    val userId = splitted(0).toLong
    val movieId = splitted(1).toInt
    val rating = splitted(2).toDouble
    val timestamp = splitted(3).toLong
    Rating(userId, movieId, rating, timestamp)
  }

  def readRatings(): List[Rating] = {
    val ratingLines = Source.fromFile("data/ratings.csv").getLines().toList.drop(1)
    val ratingList = ratingLines.map(parseRating)
    ratingList
  }

  def parseTag(line: String) = {
    val splitted = line.split(",")
    val userId = splitted(0).toInt
    val movieId = splitted(1).toInt
    val tag = splitted(2)
    val timestamp = splitted(3).toLong
    Tag(userId, movieId, tag, timestamp)
  }

  def readTags(): List[Tag] = {
    val tagLines = Source.fromFile("data/tags.csv").getLines().toList.drop(1)
    val tagList = tagLines.map(parseTag)
    tagList
  }

}
