package train

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.text.SimpleDateFormat

object AuthorProfilerTrain extends App {

	println("Starting Spark Context and Spark Session...")
	val spark = SparkSession.builder.
			      appName("Author Profiler Train").
			      getOrCreate()
	val sc = spark.sparkContext

	import spark.implicits._

	println("Importing word2vec model...")
	// import pre-trained word embeddings
	val model = sc.textFile("/user/drr342/bdad/project/word2vec/GoogleNews-vectors-negative300.txt").
					mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
	val vectors = model.map(line => line.split(" ")).
					map(tokens => (tokens(0), tokens.slice(1,tokens.length).
						map(value => value.toDouble)))
	vectors.persist

	println("Importing and tokenizing tweets...")
	// import tweets and tokenize them
	StopWordsRemover.loadDefaultStopWords("english")
	val tweets = spark.read.json("/user/drr342/bdad/project/tweets/").dropDuplicates("statusID")
	val tokenizer = new Tokenizer().setInputCol("status").setOutputCol("tokens")
	val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("filtered")
	val tokenized = remover.transform(tokenizer.transform(tweets))
	tokenized.persist

	println("Embedding tweets...")
	// change tweet tokens to average embedding vector
	val tokens = tokenized.select("filtered", "statusID").
					map(row => (row.getAs[Seq[String]](0), row.getLong(1))).rdd.
					flatMap{
					   case (keys, value) => keys.map(key => key -> value)
					}
	val embeddings = tokens.join(vectors).values.groupByKey.
						map{
							case (key, value) => (key, (value, value.size))
						}.
						mapValues(v => v._1.reduce((x, y) => (x, y).zipped.map(_ + _)).map(i => i / v._2)).
						toDF("statusID", "embeddings").
						select(col("statusID") +: (0 until 300).map(i => col("embeddings")(i).alias(s"e$i")): _*)
	embeddings.persist
	vectors.unpersist()

	println("Extracting and normalizing additional features...")
	val currentTime = new SimpleDateFormat("yyyyMMdd_HHmmss").format(System.currentTimeMillis())
	// extract other features from tweet and normalize
	val columns = Seq("favoriteCount", "hashTagsCount", 
						"mediaCount", "mentionsCount", "retweetCount", 
						"urlCount", "userFavoriteCount", "userFollowersCount", 
						"userStatusCount")
	var mean_std : Map[String, (Double, Double)] = Map()
	var features = tokenized.select("statusID", columns:_*)
	def normUDF(m:Double, s:Double) = udf{value:Long => (value - m) / s}
	columns.foreach{ column =>
		val column_rdd = tokenized.select(column).map(r => r.getLong(0)).rdd
		val (m, s) = (column_rdd.mean, column_rdd.stdev)
		mean_std += (column -> (m, s))
		features = features.withColumn("norm_" + column, normUDF(m, s)(col(column))).drop(column)
	}
	sc.parallelize(mean_std.toSeq).saveAsTextFile("/user/drr342/bdad/project/models/mean_std_" + currentTime)

	println("Joining dataframes...")
	// join label, features and embeddings
	features = tokenized.select("statusID", "category").
					join(features, "statusID").
					join(embeddings, "statusID").
					withColumnRenamed("category", "label")
	features.persist
	features.write.json("/user/drr342/bdad/project/models/features_" + currentTime)

	println("Creating datasets...")
	// create dataset in format for MLlib (label, vector of features)
	val toDrop = features.drop("statusID").columns.filter(! _.equals("label"))
	val assembler = new VectorAssembler().
		setInputCols(toDrop).
		setOutputCol("features")
	val dataset = assembler.transform(features.drop("statusID")).drop(toDrop:_*)
	dataset.persist

	tokenized.unpersist
	embeddings.unpersist

	// create datasets for each problem (age, gender, person) and split train/test
	val ageDataset = dataset.filter($"label" >= 15).
							withColumn("label", ($"label" - 15) / 10)
	val genderDataset = dataset.filter($"label" < 2)
	val personDataset = dataset.filter($"label" >= 2 && $"label" <= 11).
							withColumn("label", $"label" - 2)

	val Array(trainingAgeData, testAgeData) = ageDataset.randomSplit(Array(0.8, 0.2), seed = 1234L)
	val Array(trainingGenderData, testGenderData) = genderDataset.randomSplit(Array(0.8, 0.2), seed = 1234L)
	val Array(trainingPersonData, testPersonData) = personDataset.randomSplit(Array(0.8, 0.2), seed = 1234L)

	// WE ARE NOW READY TO EXECUTE MLLIB ALGORITHMS! :)

	println("Creating models...")
	// specify layers for the neural networks:
	val layersAge = Array[Int](309, 1000, 4)
	val layersGender = Array[Int](309, 1000, 2)
	val layersPerson = Array[Int](309, 1000, 10)

	// create the trainers and set their parameters
	val trainerAge = new MultilayerPerceptronClassifier().
							  setLayers(layersAge).
							  setBlockSize(32).
							  setSeed(1234L).
							  setMaxIter(1000)
	val trainerGender = new MultilayerPerceptronClassifier().
							  setLayers(layersGender).
							  setBlockSize(32).
							  setSeed(1234L).
							  setMaxIter(1000)
	val trainerPerson = new MultilayerPerceptronClassifier().
							  setLayers(layersPerson).
							  setBlockSize(32).
							  setSeed(1234L).
							  setMaxIter(1000)

	println("Training and testing Age model...")
	val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
	// Age model
	val modelAge = trainerAge.fit(trainingAgeData)
	val resultAge = modelAge.transform(testAgeData)
	val predictionAndLabelsAge = resultAge.select("prediction", "label")
	println(s"Test set accuracy Age = ${evaluator.evaluate(predictionAndLabelsAge)}")	
	modelAge.save("/user/drr342/bdad/project/models/age_" + currentTime)

	println("Training and testing Gender model...")
	// Gender model
	val modelGender = trainerGender.fit(trainingGenderData)
	val resultGender = modelGender.transform(testGenderData)
	val predictionAndLabelsGender = resultAge.select("prediction", "label")
	println(s"Test set accuracy Gender = ${evaluator.evaluate(predictionAndLabelsGender)}")
	modelGender.save("/user/drr342/bdad/project/models/gender_" + currentTime)

	println("Training and testing Personality model...")
	// Personality model
	val modelPerson = trainerPerson.fit(trainingPersonData)
	val resultPerson = modelPerson.transform(testPersonData)
	val predictionAndLabelsPerson = resultPerson.select("prediction", "label")
	println(s"Test set accuracy Person = ${evaluator.evaluate(predictionAndLabelsPerson)}")
	modelPerson.save("/user/drr342/bdad/project/models/person_" + currentTime)

	sc.stop()
}