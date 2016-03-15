JAVA = /usr/bin/java
JAVAC = /usr/bin/javac
CLASSPATH = .:target/classes:lib/bufmgr.jar:lib/diskmgr.jar:lib/heap.jar:lib/index.jar

all: rotest

compile: src/main/java/*/*.java
	@mkdir -p ./target/classes
	$(JAVAC) -cp $(CLASSPATH) -d ./target/classes src/main/java/*/*.java

rotest: compile
	$(JAVA) -cp $(CLASSPATH) tests.ROTest

clean:
	rm -rf target *.minibase
