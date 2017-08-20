package com.wiiaam

import java.sql.{Connection, DriverManager, SQLException}

import scala.util.Random

/**
  *
  * @param dbUrl Database url
  * @param table Table name
  */
@throws(classOf[SQLException])
class MarkovGenerator(private var dbUrl: String, private val table: String = "markov") {


  private var connection: Connection = _

  if (!dbUrl.startsWith("jdbc:sqlite:")) {
    dbUrl = "jdbc:sqlite:" + dbUrl
  }

  try {
    connection = DriverManager.getConnection(dbUrl)
    val sql = s"CREATE TABLE IF NOT EXISTS $table(" +
      " key TEXT PRIMARY KEY," +
      " value TEXT," +
      " starter BOOLEAN NOT NULL DEFAULT FALSE);"
    val stmt = connection.createStatement()
    stmt.execute(sql)
  }
  catch {
    case e: SQLException =>
      throw e
  }

  /**
    * Resets the sql table
    */
  def resetTable(): Unit = {
    var sql = s"DROP TABLE IF EXISTS $table"
    var stmt = connection.createStatement()
    stmt.execute(sql)

    sql = s"CREATE TABLE IF NOT EXISTS $table(" +
      " key TEXT PRIMARY KEY," +
      " value TEXT," +
      " starter BOOLEAN NOT NULL DEFAULT FALSE);"
    stmt = connection.createStatement()
    stmt.execute(sql)
  }

  /**
    * Prints the sql table
    */
  def printTable(): Unit = {
    val sql = s"SELECT key, value, starter FROM $table"
    val rs = connection.createStatement().executeQuery(sql)
    while (rs.next()) {
      println(rs.getString(1) + " === " + rs.getString(2) + " === " + rs.getBoolean(3))
    }
  }

  private def hasChainedWords(wordPair: String): Boolean = {
    val sql = s"SELECT key FROM $table WHERE key = ?"
    val pstmt = connection.prepareStatement(sql)
    pstmt.setString(1, wordPair)
    val rs = pstmt.executeQuery()
    rs.next()
    rs.getRow > 0
  }

  private def getChainedWordsFromPair(wordPair: String): Array[String] = {
    val sql = s"SELECT key, value FROM $table WHERE key = ?"
    val pstmt = connection.prepareStatement(sql)
    pstmt.setString(1, wordPair)
    val rs = pstmt.executeQuery()
    rs.getString("value").split("\\s+")
  }

  private def getWordPairsFromChainedWord(word: String,
                                          ignorePunctuation: Boolean = false): Array[(String, Boolean)] = {
    val sql = s"SELECT key, value, starter FROM $table"
    val pstmt = connection.prepareStatement(sql)
    val rs = pstmt.executeQuery()
    var wordPairResults = Array[(String, Boolean)]()
    while (rs.next()) {
      val chainedWords = rs.getString(2)
      if(ignorePunctuation){
        val split = chainedWords.split("\\s+")
        for(i <- split.indices){
          split(i) = split(i).replaceAll("[.,\\/#!$%\\^&\\*;:{}=\\-_`~()]", "")
        }
        if (split.contains(word))
          wordPairResults = wordPairResults :+ (rs.getString(1), rs.getBoolean(3))
      }
      else{
        if (chainedWords.split("\\s+").contains(word))
          wordPairResults = wordPairResults :+ (rs.getString(1), rs.getBoolean(3))
      }

    }
    wordPairResults

  }

  private def getReverseChainedWordsFromPair(wordPair: String): Array[(String, Boolean)] = {
    val sql = s"SELECT key, value, starter FROM $table"
    val pstmt = connection.prepareStatement(sql)
    val rs = pstmt.executeQuery()
    var wordPairResults = Array[(String, Boolean)]()
    while (rs.next()) {
      val chainedWords = rs.getString(2)
      val wordPairResult = rs.getString(1)
      val split = wordPair.split("\\s+")
      val wordPairTuple = (split(0), split(1))
      if (chainedWords.split("\\s+").contains(wordPairTuple._2) && wordPairResult.split("\\s+")(1) == wordPairTuple._1)
        wordPairResults = wordPairResults :+ (wordPairResult.split("\\s+")(0), rs.getBoolean(3))
    }
    wordPairResults

  }

  private def getAllStarters: Array[String] = {
    val sql = s"SELECT key, starter FROM $table WHERE starter = ?"
    val pstmt = connection.prepareStatement(sql)
    pstmt.setBoolean(1, true)
    val rs = pstmt.executeQuery()
    var pairs = Array[String]()
    while (rs.next()) {
      pairs = pairs :+ rs.getString(1)
    }
    pairs
  }

  private def isStarter(wordPair: String): Boolean = {
    val sql = s"SELECT key, starter FROM $table WHERE key = ?"
    val pstmt = connection.prepareStatement(sql)
    pstmt.setString(1, wordPair)
    val rs = pstmt.executeQuery()
    rs.getBoolean("starter")
  }

  private def setAsStarter(wordPair: String): Unit = {
    if (!hasChainedWords(wordPair)) return
    val sql = s"UPDATE $table SET starter = ? WHERE key = ? "
    val pstmt = connection.prepareStatement(sql)
    pstmt.setBoolean(1, true)
    pstmt.setString(2, wordPair)
    pstmt.executeUpdate()
  }

  private def chainWordToPair(wordPair: String, word: String): Unit = {
    val hasChained = hasChainedWords(wordPair)
    val sql = if (hasChained) {
      s"UPDATE $table SET value = ? WHERE key = ? "
    }
    else {
      s"INSERT INTO $table(value, key) VALUES(?,?)"
    }

    var newWords = word

    if (hasChained) {
      val chained = getChainedWordsFromPair(wordPair)
      if (chained.contains(word)) return
      for (chainedWord <- chained) {
        newWords = newWords + " " + chainedWord
      }

    }
    val pstmt = connection.prepareStatement(sql)
    pstmt.setString(1, newWords)
    pstmt.setString(2, wordPair)
    pstmt.executeUpdate()
  }


  /**
    * Parses a sentence to be included in the chain
    */
  def parseSentence(sentence: String): Unit = {
    val split = sentence.split("\\s+")
    if (split.length > 2) {
      for (i <- split.indices) {
        if (i < split.length - 2) {
          val words = Array(split(i), split(i + 1), split(i + 2))
          val wordPair = split(i) + " " + split(i + 1)
          if (words.distinct.length == words.length) chainWordToPair(wordPair, split(i + 2))
        }
      }
      val starter = split(0) + " " + split(1)
      setAsStarter(starter)
    }
  }

  /**
    *
    * @return sentence
    */
  def createSentence(): String = {
    var finished = false
    val starters = getAllStarters
    if (starters.length < 1) return "" // TODO throw an error here
    var sentence = starters(Random.nextInt(getAllStarters.length)).split("\\s+")
    var i = 0
    while (!finished) {
      val prevWordPair = sentence(i) + " " + sentence(i + 1)

      if (hasChainedWords(prevWordPair)) {
        val possibleWords = getChainedWordsFromPair(prevWordPair)
        val nextWord = possibleWords(Random.nextInt(possibleWords.length))
        sentence = sentence :+ nextWord
      }
      else {
        finished = true
      }


      if (i > 20) finished = true


      i = i + 1
    }
    sentence.deep.mkString(" ")
  }

  def createSentenceUsingWord(word: String, ignorePunctuation: Boolean = false): String = {
    val split = word.split("\\s+")
    val triggerWord = split(Random.nextInt(split.length))
    var finished = false
    var starters = getAllStarters
    if(ignorePunctuation){
      starters = starters.filter{
        s: String =>
          val split = s.split("\\s+")
          for(i <- split.indices){
            split(i) = split(i).replaceAll("[.,\\/#!$%\\^&\\*;:{}=\\-_`~()]", "")
          }
          if(split.contains(word)) true else false
      }
    }
    else{
      starters = starters.filter(_.split("\\s+").contains(triggerWord))
    }
    var wordPairResults = getWordPairsFromChainedWord(triggerWord, ignorePunctuation)
    for(starter <- starters){
      wordPairResults = wordPairResults :+ (starter, true)
    }
    if (wordPairResults.length == 0) return createSentence()

    val initialWordPair = wordPairResults(Random.nextInt(wordPairResults.length))

    val initialWordPairSplit = initialWordPair._1.split("\\s+")
    val initialWordPairSplitFiltered = initialWordPairSplit.clone()
    if(ignorePunctuation){
      for(i <- initialWordPairSplitFiltered.indices){
        initialWordPairSplitFiltered(i) = initialWordPairSplitFiltered(i).replaceAll("[.,\\/#!$%\\^&\\*;:{}=\\-_`~()]", "")
      }
    }
    var sentence = if(initialWordPairSplitFiltered.contains(triggerWord)) initialWordPairSplit
    else initialWordPairSplit :+ triggerWord
    if (!initialWordPair._2) { // Is not a starter
      while (!finished) {
        val searchPair = sentence(0) + " " + sentence(1)
        wordPairResults = getReverseChainedWordsFromPair(searchPair)
        val randomWordPair = wordPairResults(Random.nextInt(wordPairResults.length))
        sentence = randomWordPair._1.split("\\s+") ++ sentence
        if (randomWordPair._2) finished = true
      }
    }

    var i = sentence.length - 2
    finished = false
    while (!finished) {
      val prevWordPair = sentence(i) + " " + sentence(i + 1)

      if (hasChainedWords(prevWordPair)) {
        val possibleWords = getChainedWordsFromPair(prevWordPair)
        val nextWord = possibleWords(Random.nextInt(possibleWords.length))
        sentence = sentence :+ nextWord
      }
      else {
        finished = true
      }


      if (i > 20) finished = true


      i = i + 1
    }
    sentence.deep.mkString(" ")
  }


}
