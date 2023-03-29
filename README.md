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
