module purge
module load maven/3.5.2
mvn clean install

spark2-submit --class train.AuthorProfilerTrain --master yarn --deploy-mode cluster --driver-memory 2G --executor-memory 32G --num-executors 16 ./target/author-profiling-1.0.jar
