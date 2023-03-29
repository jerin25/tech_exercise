#About the application requirement
A directory in S3 contains files with two columns
1.The files contain headers, the column names are just random strings and they are not consistent across files
2.both columns are integer values
3.Some files are CSV some are TSV - so they are mixed, and you must handle this!
4.The empty string should represent 0
5.Henceforth the first column will be referred to as the key, and the second column the value
6.For any given key across the entire dataset (so all the files), there exists exactly one value that occurs an odd number of times. . E.g. you
will see data like this:

app in Scala that takes 3 parameters:
1.An S3 path (input)
2.An S3 path (output)
3.An AWS ~/.aws/credentials profile to initialise creds with, or param to indicate that creds should use default provider chain. Your app
will assume these creds have full access to the S3 bucket.
Then in spark local mode the app should write file(s) to the output S3 path in TSV format with two columns such that
The first column contains each key exactly once
The second column contains the integer occurring an odd number of times for the key.

#Building the project

mvn clean package

#Running Tests

mvn test

#Running tech_exercise in local

mkdir -p /tmp/input/ /tmp/output

#Copy sample data to input directory
cp src/main/resources/input/* /tmp/input

#Run application
spark-submit --name tech_exercise_`date +%F_%T` \
--class com.jerin.exercise.OddFindOut \
--conf spark.yarn.submit.waitAppCompletion=false  \
--master spark://localhost:7077  \
--queue testing \
target/tech_exercise-1.0-SNAPSHOT.jar \
/tmp/input /tmp/output