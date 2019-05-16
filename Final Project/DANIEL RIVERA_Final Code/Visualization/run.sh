timeStamp=$1
url=$2
path=$3

if [ -z "$timeStamp" ]
then
	echo "Usage: runTest.sh <timeStamp> <url> <savePath>"
	exit 1
fi

if [ -z "$url" ]
then
	echo "Usage: runTest.sh <timeStamp> <url> <savePath>"
	exit 1
fi

if [ -z "$path" ]
then
	echo "Usage: runTest.sh <timeStamp> <url> <savePath>"
	exit 1
fi

if $(hadoop fs -test -d $path) 
then 
	echo "Path provided already exists!"
	exit 1
fi

if ! $(hadoop fs -test -d "/user/drr342/bdad/project/models/person_${timeStamp}") 
then
	echo "There are no models associated to the given Time Stamp!"
	exit 1
fi

export BROWSER=/share/apps/firefox/45.8.0/usr/bin/firefox
module purge
module load maven/3.5.2
module load python/gnu/3.6.5
mvn clean install

spark2-submit --class test.AuthorProfilerTest --master yarn --deploy-mode cluster --driver-memory 2G --executor-memory 16G --num-executors 8 ./target/author-profiling-1.0-jar-with-dependencies.jar "$timeStamp" "$url" "$path"
cd ui
hdfs dfs -getmerge $path results.txt
python ui.py $url


