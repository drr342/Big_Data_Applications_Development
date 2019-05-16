type=$1

if [ -z "$type" ]
then
	echo "Usage: run.sh <age | gender | person>"
	exit 1
fi

if [ "$type" != "age" ] && [ "$type" != "gender" ] && [ "$type" != "person" ]
then
	echo "Usage: run.sh <age | gender | person>"
	exit 1
fi

module purge
module load maven/3.5.2
module load scala/2.11.8
mvn clean install

scala -cp ./target/data-acqusition-1.0-jar-with-dependencies.jar search2json.$type
