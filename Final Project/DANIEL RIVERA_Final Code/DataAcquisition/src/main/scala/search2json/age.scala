package search2json

import scala.collection.mutable._
import scala.collection.JavaConverters._

import twitter4j._
import twitter4j.conf._
import com.google.gson._
import com.google.gson.reflect._
import java.util.Date
import java.text.SimpleDateFormat

import java.io.FileWriter
import java.io.File

object age {

	
	def getTweets (queryString : String, age : Int, twitter : Twitter) : ListBuffer[MyTweet] = {
		
		val tweets = ListBuffer.empty[MyTweet]
		var query = new Query(queryString).count(100).lang("en")
		var result: QueryResult = null

		do {
			result = twitter.search(query)
			result.getTweets().asScala.foreach { status: Status =>
				if (!status.isRetweet()) {
					tweets += new MyTweet(
						userID = status.getUser.getId,
						user = status.getUser.getScreenName,
						userName = status.getUser.getName,
						userLocation = status.getUser.getLocation,
						statusID = status.getId,
						status = status.getText,
						hashTags = status.getHashtagEntities.map(_.getText).mkString(","),
						hashTagsCount = status.getHashtagEntities.length,
						mediaCount = status.getMediaEntities.length,
						urlCount = status.getURLEntities.length,
						mentionsCount = status.getUserMentionEntities.length,
						favoriteCount = status.getFavoriteCount,
						retweetCount = status.getRetweetCount,
						userFavoriteCount = status.getUser.getFavouritesCount,
						userFollowersCount = status.getUser.getFollowersCount,
						userFriendsCount = status.getUser.getFriendsCount,
						userStatusCount = status.getUser.getStatusesCount,
						category = age)
				}
			}
			query = result.nextQuery()
			if (result != null && result.getRateLimitStatus() != null && result.getRateLimitStatus().getRemaining() <= 0) {
				println("Rate Limit Status Reached: Waiting for %d seconds".format(result.getRateLimitStatus().getSecondsUntilReset()))
				// if (!tweets.isEmpty && save) {
				// 	write(tweets)
				// 	tweets.clear
				// }
				Thread.sleep(result.getRateLimitStatus().getSecondsUntilReset() * 1250)
			}
		} while (result == null || result.getRateLimitStatus() == null || result.hasNext())
		tweets
	}

	def write(tweets: ListBuffer[MyTweet]) = {
		println("SAVING TWEETS...")
		val gson = new GsonBuilder().serializeNulls.create
		val dateTimeFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS")
		val jsonString = gson.toJson(tweets.toList.asJava)
		val fileWriter = new FileWriter(new File("/scratch/drr342/bdad/project/tweets/age/" + dateTimeFormat.format(new Date())))
		fileWriter.write(jsonString)
		fileWriter.close()
	}
		
	def main (args: Array[String]) {
	
		val cb = new ConfigurationBuilder()
		cb.setDebugEnabled(true).setOAuthConsumerKey("")
			.setOAuthConsumerSecret("")
			.setOAuthAccessToken("-")
			.setOAuthAccessTokenSecret("")
	
		val twitter = new TwitterFactory(cb.build()).getInstance()
		val ages = 15 to 54
		val ageTweets = ListBuffer.empty[MyTweet]

		// val dirOut = new File(args(0) + "/all")
		// if (!dirOut.exists())
  //       	dirOut.mkdir();
	
		ages.foreach { age: Int =>
			
			println("COLLECTING TWEETS FOR AGE: %d".format(age))
			
			val queryAge = """"I am %1$d years old" OR
								 |"I'm %1$d years old" OR
								 |"I am %1$d yo" OR
								 |"I'm %1$d yo" OR 
								 |"I am a %1$d year old" OR 
								 |"I'm a %1$d year old" OR
								 |"I am a %1$d yo" OR
								 |"I'm a %1$d yo"""".stripMargin.replaceAll("\n", " ").format(age)
			ageTweets ++= getTweets(queryAge, age, twitter)
		}

		// if (!ageTweets.isEmpty) {
		write(ageTweets)
		// 	ageTweets.clear
		// }
		
		val age_users = ageTweets.toList.map(tweet => (tweet.category, tweet.user))
						.groupBy(tuple => tuple._1)
						.map(group => (group._1, group._2.map(tuple => tuple._2)))
		val userTweets = ListBuffer.empty[MyTweet]

		age_users.foreach { age_user: (Int, List[String]) =>
			val (age, users) = age_user
			
			println("COLLECTING TWEETS FOR USERS OF AGE: %d".format(age))
			
			users.foreach { user: String =>
				val queryUser = "from:" + user
				userTweets ++= getTweets(queryUser, age, twitter)
			}
			// if (!userTweets.isEmpty) {
			write(userTweets)
			userTweets.clear
			// }
		}
	}
}

