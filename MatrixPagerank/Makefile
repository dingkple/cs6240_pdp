DIR := $(shell basename `pwd`)
# hello
$(DIR).jar: build.gradle Makefile
	gradle build
#	gradle shadowJar
	cp build/libs/$(DIR).jar $(DIR).jar
#	zip -d $(DIR).jar LICENSE
#	zip -d $(DIR).jar /META-INF/LICENSE

run: $(DIR).jar clean-output
	hadoop jar $(DIR).jar hdfs://localhost:9000/user/kingkz/data hdfs://localhost:9000/user/kingkz/output4

clean-output:
	rm -rf output temp final_output

clean: clean-output
	rm -rf build bin *.jar .gradle test.log

.PHONY: run clean-output data clean test
