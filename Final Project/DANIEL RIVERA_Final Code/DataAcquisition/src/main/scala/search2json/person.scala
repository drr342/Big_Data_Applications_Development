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

object person {
	
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
		val fileWriter = new FileWriter(new File("/scratch/drr342/bdad/project/tweets/person/" + dateTimeFormat.format(new Date())))
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
		val keyWords = Map(
							"active"->6,
							"adventurous"->10,
							"affected"->5,
							"aggressive"->2,
							"ambitious"->8,
							"anxious"->2,
							"arrogant"->5,
							"bitter"->5,
							"blissful"->3,
							"bold"->9,
							"brave"->6,
							"calculating"->7,
							"calm"->3,
							"careless"->9,
							"cautious"->8,
							"changeable"->9,
							"chaotic"->9,
							"cheerful"->3,
							"cold"->5,
							"conciliatory"->4,
							"confident"->3,
							"conservative"->11,
							"considerate"->4,
							"conventional"->11,
							"cordial"->4,
							"coward"->11,
							"creative"->10,
							"decisive"->3,
							"depressed"->2,
							"disloyal"->5,
							"distant"->7,
							"easygoing"->3,
							"emotionally stable"->3,
							"emotionally unstable"->2,
							"evasive"->7,
							"excessive"->5,
							"extroverted"->6,
							"farsighted"->8,
							"fierce"->5,
							"fragile"->2,
							"generous"->4,
							"good-natured"->4,
							"gullible"->3,
							"happy"->6,
							"heedless"->9,
							"hopeful"->4,
							"hopeless"->5,
							"hostile"->5,
							"humble"->5,
							"imaginative"->10,
							"impulsive"->2,
							"inattentive"->5,
							"inconsiderate"->5,
							"indecisive"->2,
							"inflexible"->8,
							"insecure"->2,
							"insensible"->3,
							"interesting"->10,
							"intolerable"->7,
							"irresponsible"->9,
							"jealous"->2,
							"joyful"->3,
							"kind"->4,
							"lazy"->9,
							"liberal"->10,
							"lonely"->7,
							"loud"->6,
							"melancholic"->2,
							"messy"->9,
							"methodical"->8,
							"mistrustful"->2,
							"modest"->4,
							"monotonous"->11,
							"nervous"->2,
							"nice"->6,
							"nonproductive"->9,
							"nontraditional"->10,
							"organized"->8,
							"original"->10,
							"peaceful"->4,
							"persistent"->8,
							"petty"->5,
							"productive"->8,
							"quiet"->7,
							"rational"->3,
							"relaxed"->3,
							"reliable"->4,
							"reserved"->7,
							"responsible"->8,
							"rude"->5,
							"sad"->2,
							"secure"->3,
							"selfish"->5,
							"sensitive"->2,
							"shy"->7,
							"sincere"->4,
							"sociable"->6,
							"spontaneous"->6,
							"supportive"->4,
							"talkative"->6,
							"tense"->2,
							"tidy"->8,
							"tough"->3,
							"traditional"->11,
							"trusting"->3,
							"unconventional"->10,
							"uncreative"->11,
							"understanding"->4,
							"unhelpful"->5,
							"unkind"->5,
							"unoriginal"->11,
							"unselfish"->4,
							"unsusceptible"->3,
							"untidy"->9,
							"useless"->7,
							"vulnerable"->2,
							"warm"->4,
							"whiny"->2,
							"withdrawn"->7,
							"yielding"->9)

		val personTweets = ListBuffer.empty[MyTweet]

		// val dirOut = new File(args(0) + "/all")
		// if (!dirOut.exists())
  //       	dirOut.mkdir();
	
		keyWords.foreach { case (word, value) =>
			
			println("COLLECTING TWEETS FOR WORD: %s".format(word))
			
			val queryGender = """"I am %1$s" OR "I'm %1$s" OR "I am feeling %1$s" OR "I'm feeling %1$s"""".format(word)
			personTweets ++= getTweets(queryGender, value, twitter)
		}

		// if (!ageTweets.isEmpty) {
		write(personTweets)
		// 	ageTweets.clear
		// }
		
		val person_users = personTweets.toList.map(tweet => (tweet.category, tweet.user))
						.groupBy(tuple => tuple._1)
						.map(group => (group._1, group._2.map(tuple => tuple._2)))
		val userTweets = ListBuffer.empty[MyTweet]

		person_users.foreach { person_user: (Int, List[String]) =>
			val (person, users) = person_user
			
			println("COLLECTING TWEETS FOR USERS OF CATEGORY: %d".format(person))
			
			users.foreach { user: String =>
				val queryUser = "from:" + user
				userTweets ++= getTweets(queryUser, person, twitter)
			}
			// if (!userTweets.isEmpty) {
			write(userTweets)
			userTweets.clear
			// }
		}
	}
}

