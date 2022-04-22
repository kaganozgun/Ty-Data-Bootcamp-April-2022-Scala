import model.{Movie, Rating, Tag}

object task {

  def groupByYearAndGenre(movieList: List[Movie]): List[(Int, String, Int)]  = {
    val movieListWithExplodedGenres = movieList.flatMap(v=> v.genres.map(g => (v.id, v.title, v.year, g)))
    val groupedMovie = movieListWithExplodedGenres.groupBy(i => (i._3, i._4))
      .map{case (k,v) => (k._1.getOrElse(-1), k._2, v.length)}.toList

    groupedMovie.sortBy(r => (r._1, r._3)).reverse
  }

  def goldenYearOfCinema(ratingList: List[Rating], movieList: List[Movie]): List[(Int, Double)]  = {
    val avgRatingByMovie = ratingList.groupBy(i => i.movieId)
      .map{ case (k,v) => (k, v.map(_.rating).sum / v.length)}.toList
    val movieMap = movieList.map(k => (k.id, k)).toMap
    val avgRatingWithMovieDetails = avgRatingByMovie.map(ar => (movieMap(ar._1), ar._2))
    val yearAvgRating = avgRatingWithMovieDetails.groupBy(k => k._1.year)
      .map{case (k,v) => (k.getOrElse(-1), v.map(_._2).sum / v.length)}.toList
    yearAvgRating
  }

  def getMinGenreByYears(movieList: List[Movie]): List[(Int, String, Int, Int, Int)] = {
    val genreFlatten = movieList.flatMap(v=> v.genres.map(g => (v.id, v.title, v.year, g)))
    val movieCntByGenre = genreFlatten.groupBy(k => k._4).map{case (k,v) => (k, v.length)}
    val totalMovieCnt = movieList.length
    val minGenreByYear = genreFlatten.groupBy(i => (i._3, i._4)).map{case (k,v) => (v.length, k._1.getOrElse(-1), k._2)}
      .groupBy(j => j._2).map{z => (z._1, z._2.map(x=> x).min)}
    val minGenreByYearWithDetails = minGenreByYear
      .map{k=> (k._1, k._2._3, k._2._1, movieCntByGenre.get(k._2._3).getOrElse(-1), totalMovieCnt)}
      .toList.sortBy(_._1)
    minGenreByYearWithDetails
  }

  def calculateNumberOfTagAndVote(movieList: List[Movie], ratingList: List[Rating], tagList: List[Tag]):
  List[(String, Int, Int, Int)] = {
    val movieMap = movieList.map(i => (i.id, i)).toMap
    val ratingMap = ratingList.map(i => ((i.userId, i.movieId), i)).toMap

    val userTagRating  = tagList
      .flatMap{ j => movieMap.get(j.movieId).get.genres
        .map( g => (j.userId, g, movieMap.get(j.movieId).get.year.getOrElse(-1), j.tag,
          ratingMap.get((j.userId, j.movieId)).getOrElse(-1)))}

    val userGiveTagAndVote = userTagRating.groupBy(j => (j._2, j._3)).map{case (k,v) => (k._1, k._2, v.length)}
      .map(i => ((i._1,  i._2), i._3)).toMap
    val userGiveTag = userTagRating.filter(_._5 != -1).groupBy(j => (j._2, j._3)).map{case (k,v) => (k._1, k._2, v.length)}
      .map(i => ((i._1, i._2), i._3)).toMap

    val voteAndTagCntAndRatioGroupedByGenreAndYear = userGiveTagAndVote
      .map{k => (k._1, userGiveTag.get(k._1).getOrElse(0), k._2 ,if (userGiveTag.get(k._1).getOrElse(0) == 0)
        if(k._2 > 0) 1.0 else 0.0
      else userGiveTag.get(k._1).get.toDouble / k._2.toDouble)}.map{y => (y._1._1, y._1._2, y._2, y._3, y._4)}.toList

    val maxRateByGenre = voteAndTagCntAndRatioGroupedByGenreAndYear.groupBy(i => i._1)
      .map{case (k,v) => (k, v.map{x => x}.maxBy(_._5)._2)}

    voteAndTagCntAndRatioGroupedByGenreAndYear.groupBy(i => i._1)
      .map{case (k,v) => (k,  v.map(_._3).sum, v.map(_._4).sum, maxRateByGenre.get(k).getOrElse(-1))}.toList
  }

  def getMostVotedUserMetrics(tagList: List[Tag], movieList: List[Movie], ratingList: List[Rating]):
  (Int, (Int,String,Double),(Int,String,Double)) = {
    val userWithMaxTag = tagList.groupBy(i => i.userId).map{case (k,v) => (k, v.length)}.maxBy(_._2)
    val userId = userWithMaxTag._1
    val voteCnt = userWithMaxTag._2

    val movieMap = movieList.map(i => (i.id, i)).toMap
    val movieRating = ratingList.filter(_.userId == userId)
      .map{x => (movieMap.get(x.movieId).get, x.rating)}

    val moviesWithExplodedGenre = movieRating
      .flatMap(v=> v._1.genres.map(g => (v._1.year.getOrElse(-1), g, v._2)))

    val yearGenreAvgRating = moviesWithExplodedGenre.groupBy(i => (i._1, i._2))
      .map{case (k,v) => (k._1, k._2, v.map(_._3).sum / v.map(_._3).length.toDouble)}

    (voteCnt, yearGenreAvgRating.minBy(_._3), yearGenreAvgRating.maxBy(_._3))
  }

  def getFirstEventTypeByGenre(tagList: List[Tag], ratingList: List[Rating], movieList: List[Movie])
  : List[(String, String)] = {
    val movieIdMinTagTs = tagList.groupBy(i => i.movieId).map{case (k,v) => (k, v.map(_.timestamp).min)}
      .map(i => (i._1, i))
    val movieIdMinRatingTs = ratingList.groupBy(i => i.movieId).map{case (k,v) => (k, v.map(_.timestamp).min)}
      .map(i => (i._1, i))

    val movieGenreFirstEvent = movieList.flatMap(k => k.genres.map(g => (k.id, g)))
      .map{i => (i._1, i._2, if (movieIdMinTagTs.get(i._1).map(_._2).getOrElse(Long.MaxValue) >
        movieIdMinRatingTs.get(i._1).map(_._2).getOrElse(Long.MaxValue)) 1
      else -1)}

    movieGenreFirstEvent.groupBy(i => i._2).map{case (k,v) => (k, v.map(_._3).sum)}
      .map{ g => (g._1, if (g._2 > 0) "Rating First" else if (g._2 < 0) "Tag First" else "Equal")}.toList
  }

}
