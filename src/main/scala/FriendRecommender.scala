import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class userDetail(UserId: String, Friends: Set[String], DateOfInt: Date)
object FriendRecommender  {


  val DATE_FORMAT: String = "yyyyMMddhh"
  val dateFormat = new SimpleDateFormat(DATE_FORMAT)
  val formatter = new SimpleDateFormat("MMM");


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("FriendRecommendation").master("local[*]").getOrCreate()

    Logger.getRootLogger.setLevel(Level.OFF)
    spark.sparkContext.setLogLevel(logLevel = "ERROR")

    val dataFiles = spark.sparkContext.wholeTextFiles("C:\\Users\\gopasali\\Documents\\sampleUserInterations.txt")

    import spark.implicits._

    val mappedRDD: RDD[(mutable.Map[String, Set[String]], Seq[(String, String)], Map[(String, Int), mutable.Map[(String, String), Int]])] = dataFiles.map(_._2).map(dataFile => {

      val indFriends: mutable.Map[String, List[String]] = mutable.Map.empty
      val connection: mutable.Map[String, Set[String]] = mutable.Map.empty

      val dataLines = dataFile.split("\n")
      val arrBuffer = mutable.ArrayBuffer[userDetail]()

      dataLines.foreach(line => {
        val arrayData = line.split(" ")
        val head: String = arrayData.head
        val last = arrayData.last.trim
        val intermediateData = arrayData.tail.take(arrayData.length - 2).toList

        if (indFriends.contains(head)) {
          indFriends.put(head, indFriends(head) ++ intermediateData)
          arrBuffer += userDetail(head, intermediateData.toSet, dateFormat.parse(last))
        } else {
          arrBuffer += userDetail(head, intermediateData.toSet, dateFormat.parse(last))
          indFriends.put(head, intermediateData)
        }
      })

      connection ++= indFriends.mapValues(_.toSet)
      val mutualFriendRecomm: Seq[(String, String)] = getMutualFriendConnection(indFriends)(connection)
      val weeklyData: Map[(String, Int), mutable.Map[(String, String), Int]] = getWeeklyData(arrBuffer)
      val sortedValues: Seq[(String, String)] = mutualFriendRecomm.sortBy(_._1)

      (connection,sortedValues,weeklyData)
    }).cache()

    val friendConnection: RDD[(String, String)] = mappedRDD.flatMap(_._1).flatMap(data => {
      data._2.map(friend => (data._1,friend))
    })

    val firendConnDf = friendConnection.toDF("user","Friend")

    firendConnDf.write.mode(SaveMode.Overwrite).option("header", "true").csv("C:\\Users\\gopasali\\IdeaProjects\\FriendSuggestor\\out\\connection\\")

    val weeklyInteraction: RDD[(String, Int, String, String, Int)] = mappedRDD.flatMap(_._3).flatMap(data =>{

      data._2.toList.map(userData =>{
        (data._1._1,data._1._2,userData._1._1, userData._1._2,userData._2)

      })
    })



    val weekIntDF: DataFrame = weeklyInteraction.toDF("Month","Week","User","Friend","InteractedCount")

    weekIntDF.show()

    val userInteraction: Dataset[Row] = weekIntDF.orderBy("User")
    userInteraction.write.mode(SaveMode.Overwrite).option("header", "true").csv("C:\\Users\\gopasali\\IdeaProjects\\FriendSuggestor\\out\\userInteraction\\")


    val userInteractionFiltered: Dataset[Row] = weekIntDF.where("InteractedCount > 2")
    userInteractionFiltered.write.mode(SaveMode.Overwrite).option("header", "true").csv("C:\\Users\\gopasali\\IdeaProjects\\FriendSuggestor\\out\\userInteractionFiltered\\")

    val mutualConnection: RDD[(String, String)] = mappedRDD.flatMap(_._2)

    val mutualDf: DataFrame =  mutualConnection.toDF("user","mutual_suggestion")
    mutualDf.write.mode(SaveMode.Overwrite).option("header", "true").csv("C:\\Users\\gopasali\\IdeaProjects\\FriendSuggestor\\out\\mutualSuggestion\\")


  }

  def getMutualFriendConnection(indFriends: mutable.Map[String, List[String]])(connection: mutable.Map[String, Set[String]]): List[(String, String)] = {

    val mutualFriendRecomm = indFriends.toList.flatMap(data => {

      val friend = data._1
      val listOfFriends = data._2.flatMap(refFrnd => {

        val filteredMap = indFriends.filterNot(_._1.equals(friend))
        val TotalFriends = filteredMap.flatMap(data => {
          if (data._1.equals(refFrnd)) data._2
          else if (data._2.contains(friend)) {
            connection.put(friend, connection(friend) + data._1)
            List.empty
          }
          else if (data._2.contains(refFrnd)) List(data._1)
          else List.empty
        })

        TotalFriends
      })
      val duplMap = mutable.Map.empty[String, Int]

      listOfFriends.foreach(friend => {
        if (duplMap.contains(friend)) duplMap.put(friend, duplMap(friend) + 1)
        else duplMap.put(friend, 1)

      })
      duplMap.toList.filter(_._2 > 1).map(_._1).filterNot(name => data._2.contains(name)).map(value => (friend, value))
    })

    mutualFriendRecomm
  }



  def getWeeklyData(arrBuffer: ArrayBuffer[userDetail]): Map[(String, Int), mutable.Map[(String, String), Int]] = {
    val weeklyConnection: Map[(String, Int), List[userDetail]] = arrBuffer.toList.groupBy(user => {
      val ca1 = Calendar.getInstance()
      ca1.setTime(user.DateOfInt)
      val month = formatter.format(user.DateOfInt)
      ca1.setMinimalDaysInFirstWeek(1)
      val wk = ca1.get(Calendar.WEEK_OF_MONTH)
      (month, wk)
    })


    val weeklyData: Map[(String, Int), mutable.Map[(String, String), Int]] = weeklyConnection.mapValues(userList => {
      val flatternUsers = userList.flatMap(user => {
        user.Friends.map(friend => (user.UserId, friend))
      })
      val UserInteractionCount = mutable.Map.empty[(String, String), Int]
      flatternUsers.foreach(interactions => {
        if (UserInteractionCount.contains((interactions._1, interactions._2)) || UserInteractionCount.contains((interactions._2, interactions._1))) {
          val interactionCount = UserInteractionCount.getOrElse((interactions._1, interactions._2), UserInteractionCount(interactions._2, interactions._1))
          UserInteractionCount.put((interactions._1, interactions._2), interactionCount + 1)
        }

        else {
          UserInteractionCount.put((interactions._1, interactions._2), 1)
        }

      })
      UserInteractionCount
    })

    weeklyData
  }

}



