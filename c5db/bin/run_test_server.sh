#!/bin/sh
set -ex
MAVEN_JAR=target/c5db-${project.version}-jar-with-dependencies.jar
if [ -f ${MAVEN_JAR} ]
then
	echo "skiping maven"
else
	mvn assembly:single
fi
java -cp ${MAVEN_JAR}:target/c5db-${project.version}.jar c5db.Main
