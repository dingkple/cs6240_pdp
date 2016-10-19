DIR := $(shell basename `pwd`)

$(DIR).jar: build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/$(DIR)-all.jar $(DIR).jar
	zip -d $(DIR).jar LICENSE
	zip -d $(DIR).jar /META-INF/LICENSE

run: $(DIR).jar clean-output
	hadoop jar $(DIR).jar data

clean-output:
	rm -rf data? dbrata?? output

clean: clean-output
	rm -rf build bin *.jar .gradle test.log

.PHONY: run clean-output data clean test
