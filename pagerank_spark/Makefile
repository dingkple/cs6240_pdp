DIR := $(shell basename `pwd`)
$output = "output"
$input = "input"

$(DIR).jar: build.sbt Makefile
	# gradle build
	# gradle shadowJar
	# cp build/libs/$(DIR)-all.jar $(DIR).jar
	# zip -d $(DIR).jar LICENSE
	# zip -d $(DIR).jar /META-INF/LICENSE
	sbt clean
	sbt package
	cp target/scala-2.10/root_new_2.10-1.1.1.jar $(DIR).jar

run: 
	# $(DIR).jar clean-output
	# fill the arguments here
	# spark-submit --class pagerank.Pagerank 
	java $(DIR).jar $(input) $(output)

clean-output:
	rm -rf output
	sbt clean

clean: clean-output
	sbt clean

.PHONY: run clean-output data clean test
