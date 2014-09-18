#!/bin/sh
set -ex
MAVEN_JAR=target/c5db-${project.version}-jar-with-dependencies.jar
if [ -f ${MAVEN_JAR} ]
then
	echo "skiping maven"
else
	mvn assembly:single
fi
java -DwebServerPort=31337 -DregionServerPort=8083 -cp ${MAVEN_JAR}:target/c5db-${project.version}.jar c5db.Main  &
java -DwebServerPort=31338 -DregionServerPort=8081 -cp ${MAVEN_JAR}:target/c5db-${project.version}.jar c5db.Main  &
java -DwebServerPort=31339 -DregionServerPort=8082 -cp ${MAVEN_JAR}:target/c5db-${project.version}.jar c5db.Main  &
