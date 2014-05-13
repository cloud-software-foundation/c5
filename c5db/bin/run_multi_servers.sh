#!/bin/sh
set -ex
MAVEN_JAR=target/c5db-0.1-SNAPSHOT-jar-with-dependencies.jar
if [ -f ${MAVEN_JAR} ]
then
	echo "skiping maven"
else
	mvn assembly:single
fi
java -DwebServerPort=31337 -DregionServerPort=8083 -cp ${MAVEN_JAR}:target/c5db-0.1-SNAPSHOT.jar c5db.Main  &
java -DwebServerPort=31338 -DregionServerPort=8081 -cp ${MAVEN_JAR}:target/c5db-0.1-SNAPSHOT.jar c5db.Main  &
java -DwebServerPort=31339 -DregionServerPort=8082 -cp ${MAVEN_JAR}:target/c5db-0.1-SNAPSHOT.jar c5db.Main  &
