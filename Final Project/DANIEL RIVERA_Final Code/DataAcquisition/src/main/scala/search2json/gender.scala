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

object gender {
	
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
		val fileWriter = new FileWriter(new File("/scratch/drr342/bdad/project/tweets/gender/" + dateTimeFormat.format(new Date())))
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
		val keyWords = Map("boy"->0, "man"->0, "guy"->0, "girl"->1, "gal"->1, "woman"->1)
		val genderTweets = ListBuffer.empty[MyTweet]

		// val dirOut = new File(args(0) + "/all")
		// if (!dirOut.exists())
  //       	dirOut.mkdir();
	
		keyWords.foreach { case (word, value) =>
			
			println("COLLECTING TWEETS FOR WORD: %s".format(word))
			
			val queryGender = """"I am a %1$s" OR "I'm a %1$s"""".format(word)
			genderTweets ++= getTweets(queryGender, value, twitter)
		}

		// if (!ageTweets.isEmpty) {
		write(genderTweets)
		// 	ageTweets.clear
		// }
		
		val gender_users = genderTweets.toList.map(tweet => (tweet.category, tweet.user))
						.groupBy(tuple => tuple._1)
						.map(group => (group._1, group._2.map(tuple => tuple._2)))
		val userTweets = ListBuffer.empty[MyTweet]

		gender_users.foreach { gender_user: (Int, List[String]) =>
			val (gender, users) = gender_user
			
			println("COLLECTING TWEETS FOR USERS OF CATEGORY: %d".format(gender))
			
			users.foreach { user: String =>
				val queryUser = "from:" + user
				userTweets ++= getTweets(queryUser, gender, twitter)
			}
			// if (!userTweets.isEmpty) {
			write(userTweets)
			userTweets.clear
			// }
		}
	}
}

