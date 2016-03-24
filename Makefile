JAVA = /usr/bin/java
JAVAC = /usr/bin/javac
CLASSPATH = .:..:lib/bufmgr.jar:lib/diskmgr.jar:lib/heap.jar:lib/index.jar

all: xx

compile: src/main/java/*/*.java
	@mkdir -p ./bin
	$(JAVAC) -cp $(CLASSPATH) -d ./bin src/main/java/*/*.java

xx: compile
	$(JAVA) -cp $(CLASSPATH):bin tests.ROTest
	$(JAVA) -cp $(CLASSPATH):bin tests.QEPTest src/SampleData

clean:
	rm -rf bin *.minibase
