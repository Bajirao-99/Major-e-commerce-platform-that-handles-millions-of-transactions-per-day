question3: question3.cpp
	g++ -o question3 question3.cpp 

run1: question3
	./question3 < input1.txt

run2: question3
	./question3 < input2.txt