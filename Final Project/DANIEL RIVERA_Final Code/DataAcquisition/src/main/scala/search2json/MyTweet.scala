package search2json

case class MyTweet(
	userID: Long,
	user: String,
	userName: String,
	userLocation: String,
	statusID: Long,
	status: String,
	//statusPlace: String,
	hashTags: String,
	hashTagsCount: Int,
	mediaCount: Int,
	urlCount: Int,
	mentionsCount: Int,
	favoriteCount: Int,
	retweetCount: Int,
	userFavoriteCount: Int,
	userFollowersCount: Int,
	userFriendsCount: Int,
	userStatusCount: Int,
	category: Int
)