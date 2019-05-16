package test

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import twitter4j._
import twitter4j.conf._
import scala.collection.JavaConverters._

object AuthorProfilerTest extends App {
	if (args.length < 3) {
		println("Usage: AuthorProfilerTest <timeStamp> <tweetURL> <savePath>")
		sys.exit(1)
	}

	val timeStamp = args(0)
	val tweetID = args(1).substring(args(1).lastIndexOf("/") + 1, args(1).size).toLong
	val savePath = args(2)
	// val timeStamp = "20190417_235312"
	// val tweetID = 1118713931424419841L

	println("Starting Spark Context and Spark Session...")
	val spark = SparkSession.builder.
			      appName("Author Profiler Test").
			      getOrCreate()
	val sc = spark.sparkContext

	import spark.implicits._

	println("Retrieving tweet...")
	val cb = new ConfigurationBuilder()
	cb.setDebugEnabled(true).setOAuthConsumerKey("").
		setOAuthConsumerSecret("").
		setOAuthAccessToken("-").
		setOAuthAccessTokenSecret("")
	val twitter = new TwitterFactory(cb.build()).getInstance()
	val result = twitter.lookup(tweetID).asScala
	if (result.isEmpty) {
		println("Tweet not found! Program will exit...")
		sys.exit(1)
	}
	val status = result(0)
	val tweet = new MyTweet(
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
						category = -1)

	println("Importing word2vec model and norm parameters...")
	// import pre-trained word embeddings
	val model = sc.textFile("/user/drr342/bdad/project/word2vec/GoogleNews-vectors-negative300.txt").
					mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
	val vectors = model.map(line => line.split(" ")).
					map(tokens => (tokens(0), tokens.slice(1,tokens.length).
						map(value => value.toDouble)))
	val mean_std = sc.textFile("bdad/project/models/mean_std_" + timeStamp).
					map(line => line.replaceAll("[()]","").split(",")).
					map(tokens => (tokens(0), (tokens(1).toDouble, tokens(2).toDouble))).
					collectAsMap
	vectors.persist

	println("Extracting features from tweet...")
	val tweetDF = Seq(tweet).toDF
	val tokenizer = new Tokenizer().setInputCol("status").setOutputCol("tokens")
	val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("filtered")
	val tokenized = remover.transform(tokenizer.transform(tweetDF))
	val tokens = tokenized.select("filtered", "statusID").
					map(row => (row.getAs[Seq[String]](0), row.getLong(1))).rdd.
					flatMap{
					   case (keys, value) => keys.map(key => key -> value)
					}

	println("Embedding tweet...")
	val embeddings = tokens.join(vectors).values.groupByKey.
						map{
							case (key, value) => (key, (value, value.size))
						}.
						mapValues(v => v._1.reduce((x, y) => (x, y).zipped.map(_ + _)).map(i => i / v._2)).
						toDF("statusID", "embeddings").
						select(col("statusID") +: (0 until 300).map(i => col("embeddings")(i).alias(s"e$i")): _*)
	embeddings.persist
	vectors.unpersist()
	val columns = Seq("favoriteCount", "hashTagsCount", 
					"mediaCount", "mentionsCount", "retweetCount", 
					"urlCount", "userFavoriteCount", "userFollowersCount", 
					"userStatusCount")
	var features = tokenized.select("statusID", columns:_*)
	def normUDF(m:Double, s:Double) = udf{value:Long => (value - m) / s}
	mean_std.foreach{ case(key, value) =>
		features = features.withColumn(key, normUDF(value._1, value._2)(col(key)))
	}
	features = tokenized.select("statusID").
					join(features, "statusID").
					join(embeddings, "statusID")
	features.persist
	embeddings.unpersist()

	println("Creating vector...")
	// create dataset in format for MLlib (label, vector of features)
	val toDrop = features.drop("statusID").columns
	val assembler = new VectorAssembler().
							setInputCols(toDrop).
							setOutputCol("features")
	val vector = assembler.transform(features.drop("statusID")).drop(toDrop:_*)

	println("Loading trained models and performing predictions...")
	val modelAge = MultilayerPerceptronClassificationModel.load("/user/drr342/bdad/project/models/age_" + timeStamp)
	val predictAge = modelAge.transform(vector).select("probability", "prediction").toJSON.rdd
	val modelGender = MultilayerPerceptronClassificationModel.load("/user/drr342/bdad/project/models/gender_" + timeStamp)
	val predictGender = modelGender.transform(vector).select("probability", "prediction").toJSON.rdd
	val modelPerson = MultilayerPerceptronClassificationModel.load("/user/drr342/bdad/project/models/person_" + timeStamp)
	val predictPerson = modelPerson.transform(vector).select("probability", "prediction").toJSON.rdd

	println("Loading training data...")
	val trainingData = spark.read.json("/user/drr342/bdad/project/models/features_" + timeStamp)
	trainingData.persist
	val toDropTraining = trainingData.drop("statusID").drop("label").columns
	val assemblerTraining = new VectorAssembler().
							setInputCols(toDropTraining).
							setOutputCol("features")
	val vectorsTraining = assemblerTraining.transform(trainingData).drop(toDropTraining:_*)
	val vectorNormalized = vector.collect.
							map(_.getAs[org.apache.spark.ml.linalg.Vector](0)).
							map{vec => 
								val norm = Vectors.norm(Vectors.fromML(vec), 2.0)
								val arr = vec.toArray.map(e => e / norm)
								Vectors.dense(arr)
							}
	def simUDF(v1:org.apache.spark.mllib.linalg.Vector) = udf{  v2:org.apache.spark.ml.linalg.Vector =>
																val norm = Vectors.norm(Vectors.fromML(v2), 2.0)
																val arr = v2.toArray.map(e => e / norm)
																Vectors.sqdist(v1, Vectors.dense(arr))
															}
	val similarity = sc.parallelize(
						vectorsTraining.withColumn("similarity", simUDF(vectorNormalized(0))(col("features"))).
						drop("features").drop("label").sort("similarity").take(5).
						map(r => r.getAs[Long]("statusID"))).
						map(x => x.toString)

	// println("Prediction for age: " + predictAge)
	// println("Prediction for gender: " + predictGender)
	// println("Prediction for personality: " + predictPerson)
	// println("Top 5 most similar tweets: " + similarity)

	predictAge.union(predictGender).union(predictPerson).union(similarity).saveAsTextFile(savePath)

	sc.stop()
}