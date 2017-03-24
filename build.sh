cd $COMPONENT
javac -d classes src/qp/utils/*.java
javac -d classes src/qp/parser/*.java 
javac -d classes src/qp/operators/*.java 
javac -d classes src/qp/optimizer/*.java 
javac -d classes testcases/*.java 
javac -d classes src/QueryMain.java 