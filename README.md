HDFS Writer
===========

Author: nirmal070125@gmail.com

This is a utility that you could use to write a given file to a HDFS.

How to run
==========

Arguments;

-Dhdfs.url=hdfs://10.x.x.x:9000 : HDFS URL - default value is hdfs://localhost:9000
-Dfile.path=/tmp/abc.txt : Path of the file to be written to HDFS.
-Dhdfs.path=/tmp/xyz.txt : Target path that the file will be written to.

Example:

mvn clean install -Dhdfs.url=hdfs://10.10.100.1:9000 -Dfile.path=/tmp/xyz.csv -Dhdfs.path=/nirmal/abc.csv
