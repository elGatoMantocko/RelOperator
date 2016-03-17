JAVA = /usr/bin/java
JAVAC = /usr/bin/javac
CLASSPATH = .:target/classes:bin:lib/bufmgr.jar:lib/diskmgr.jar:lib/heap.jar:lib/index.jar

all: rotest

compile: src/main/java/*/*.java
	@mkdir -p ./bin
	$(JAVAC) -cp $(CLASSPATH) -d ./bin src/main/java/*/*.java

rotest: compile
	$(JAVA) -cp $(CLASSPATH) tests.ROTest
	$(JAVA) -cp $(CLASSPATH) tests.QEPTest

clean:
	rm -rf bin *.minibase
