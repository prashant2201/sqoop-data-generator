Sqoop Data generator tool
=========================

Simple mapreduce job for generating random data that can be used in Sqoop testing (functional, performance, ...).

Compilation
-----------

Project is using maven without any special dependencies, so simply type:

    mvn package

Usage
-----

Run the jar as an hadoop application:
  
    hadoop jar sqoop-data-generator-1.0.jar net.jarcec.sqoop.data.gen.Generator PARAMETERS

The generation tool expects 4 arguments:

* Output directory on HDFS where to store generated output. Unlike usual mapreduce, this directory will be removed if it already exists.
* Comma separated list of columns that should be generated. Please check out list of supported types below.
* Number of files that should be created. This number will equal to number of used mappers.
* Number of lines (rows) that each file should generate.

Example:
 
    hadoop jar sqoop-data-generator-1.0.jar net.jarcec.sqoop.data.gen.Generator /user/jarcec/data id,i,f,s50 10 100

Supported column types:

* id - integer based column that will go from 1 to number of generated rows. This number will be unique across all generated files and thus can be used in place of numerical primary key.
* s50 - string of length 50
* s255 - string of length 255
* i - integer
* f - float (or decimal)
* t - time
* d - date
* dt - date time
