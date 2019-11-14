
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

//case class userDetail(UserId: String, Friends: Set[String], DateOfInt: Date)

object StartRecommendation {


  val DATE_FORMAT: String = "yyyyMMddhh"
  val dateFormat = new SimpleDateFormat(DATE_FORMAT)

  def main(args: Array[String]): Unit = {


    val source = Source.fromFile("C:\\Users\\gopasali\\Documents\\testFriends.txt")
    val dataFile: List[String] = source.getLines().toList


    val indFriends = mutable.Map.empty[String, List[String]]
    val arrBuffer = mutable.ArrayBuffer[userDetail]()

    dataFile.foreach(line => {
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


    val weeklyConnection = arrBuffer.toList.groupBy(user => {
      val ca1 = Calendar.getInstance()
      ca1.setTime(user.DateOfInt)
      val month = ca1.get(Calendar.MONTH)
      ca1.setMinimalDaysInFirstWeek(1)
      val wk = ca1.get(Calendar.WEEK_OF_MONTH)
      (month, wk)
    })


   val weeklyData: Map[(Int, Int), mutable.Map[(String, String), Int]] =  weeklyConnection.mapValues(userList => {
      val flatternUsers = userList.flatMap(user => {
        user.Friends.map(friend => (user.UserId, friend))
      })
      val UserInteractionCount = mutable.Map.empty[(String, String), Int]
      flatternUsers.foreach(interactions => {
        if (UserInteractionCount.contains((interactions._1, interactions._2)) || UserInteractionCount.contains((interactions._2, interactions._1))) {
         val interactionCount = UserInteractionCount.getOrElse((interactions._1, interactions._2),UserInteractionCount(interactions._2, interactions._1))
          UserInteractionCount.put((interactions._1, interactions._2), interactionCount + 1)
        }

        else {
          UserInteractionCount.put((interactions._1, interactions._2), 1)
        }

      })
     UserInteractionCount
    })


    println(weeklyData.mkString("||"))

//    val lastWeekData: ((Int, Int), List[userDetail]) = weeklyConnection.toList.maxBy(_._1)


    val connection = mutable.Map.empty[String, Set[String]]
    connection ++= indFriends.mapValues(_.toSet)

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
        if (duplMap.contains(friend)) {
          duplMap.put(friend, duplMap(friend) + 1)
        }
        else {
          duplMap.put(friend, 1)
        }
      })


      val mapped = duplMap.toList.filter(_._2 > 1).map(_._1).filterNot(name => data._2.contains(name)).map(values => (friend, values))
      mapped
    })

    val sortedValues = mutualFriendRecomm.sortBy(_._1)
    val connectedFriends = connection.map(tuple => {
      s"${tuple._1} -> ${tuple._2.mkString(",")}"
    }).mkString("\n")

    println(connectedFriends)
    //    println(sortedValues.mkString("\n"))
    source.close()

  }


}


/* val spark = SparkSession.builder().appName("FriendRecommendation").master("local").getOrCreate()
    Logger.getRootLogger.setLevel(Level.OFF)






    import spark.implicits._
   val dataset =  spark.sparkContext.parallelize(data).flatMap(value => {
      val arrayData = value.split(" ")
      val head: String = arrayData.head
      val last = arrayData.last
      val intermediateData = arrayData.tail.take(arrayData.length - 2)
     intermediateData.map((_,head))
    }).toDF("friend1","friend2")


    dataset.show()*/