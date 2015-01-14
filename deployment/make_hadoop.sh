#!/bin/bash
echo 'Compilar i crear JAR'

cd ..
mvn package -Pdist,src,native,tar -DskipTests # 2> make_errors.txt > make_output.txt

if [ $# -ne 0 ]
  then
    ./deployment/deploy_hadoop_to_VM.sh
fi

