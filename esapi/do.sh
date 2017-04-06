#!/bin/bash

cd `dirname $0`
dir=`pwd`
targetJar=esloader.jar
CLASSPATH=".:conf:/etc/elasticsearch1/conf"

getClasspath() {
	libs="/usr/lib/hive/lib /usr/lib/hadoop /usr/lib/hadoop-hdfs /usr/lib/hadoop/lib /usr/lib/hbase /usr/lib/elasticsearch/elasticsearch-2.0.0-transwarp/lib"
	libs2="$dir/libs"
	for libdir in $libs
	do
	    #echo $libdir
	    for file in `ls $libdir/*.jar`
	    do
	        #echo $file
	        CLASSPATH+=":$file"
	    done
	done

	#echo "CLASSPATH=$CLASSPATH"
}

compile() {
	getClasspath

	#svn update

	if [ -e classes ]; then
	    rm classes/* -rf
	else
	    mkdir classes
	fi

	FILELIST=""
	for javafile in `find src |grep "\.java$"`
	do
	    FILELIST+="$javafile "
	done
	javac -verbose -sourcepath src/main/java -cp $CLASSPATH -d classes $FILELIST
	FILELIST=""
	for scalafile in `find src |grep ".scala$"`
	do
	    FILELIST+="$scalafile "
	done
	#scalac -verbose -sourcepath src/main/scala -cp $CLASSPATH -d classes $FILELIST

	rm $targetJar -f
	cd classes
	jar cvf ../$targetJar .
	cd -
}

run() {
	#if [ $# -lt 2 ]
	#then
		echo "Usage: $0 run [<batchsize(=500)> <concurrent(=5)> <data-dir(=$dir/data)> <topic(=xx_test) <confirm(=true)]"
	#	exit 1
	#fi

	echo $*
	getClasspath

	batchSize=${1:-500}
	concurrent=${2:-5}
	dataDir=${3:-$dir/data}
	topic=${4:-xx_test}
	confirm=${5:-true}

	if [ 'X'$confirm != "Xfalse" ]; then
	    read -p "confirm to import use batchSize=$batchSize, concurrent=$concurrent, data dir=$dataDir, topic=$topic, is that OK [Yes|No]?" CONFIRM
	    [ 'X'$CONFIRM != "XYes" ] && echo "Your answer is not Yes, import aborted." && exit 1
	fi

	JAVA_OPTS="$JAVA_OPTS -Xms10g -Xmx10g"
	JAVA_OPTS="$JAVA_OPTS -Dcluster.name=elasticsearch1"
	JAVA_OPTS="$JAVA_OPTS -Ddiscovery.zen.ping.unicast.hosts=idcisp-datanode-141,idcisp-datanode-142,idcisp-datanode-143,idcisp-datanode-144,idcisp-datanode-145,idcisp-datanode-147,idcisp-datanode-148,idcisp-datanode-149"
	JAVA_OPTS="$JAVA_OPTS -Dbulk.import.fstype=LOCAL"
	JAVA_OPTS="$JAVA_OPTS -Dbulk.import.compression=GZ"
	JAVA_OPTS="$JAVA_OPTS -Dbulk.import.creator=io.transwarp.client.EricssonCreator"
	JAVA_OPTS="$JAVA_OPTS -Dbulk.import.batchsize=$batchSize"
	JAVA_OPTS="$JAVA_OPTS -Dbulk.import.concurrent=$concurrent"
	JAVA_OPTS="$JAVA_OPTS -Dbulk.import.data.local.dir=$dataDir"
	JAVA_OPTS="$JAVA_OPTS -Dbulk.import.index.name=$topic"
	JAVA_OPTS="$JAVA_OPTS -Djava.library.path=/usr/lib64"
	JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=log4j.properties"

	$JAVA_HOME/bin/java $JAVA_OPTS -cp $targetJar:$CLASSPATH io.transwarp.client.Importer $@ 2>&1
}

case "$1" in
	compile)
		compile
		;;
	run)
		params=$*
		params=${params#*run}
		run $params
		;;
	*)
		echo "Usage: $0 <compile | run>" >&2
		exit 1
		;;
esac
