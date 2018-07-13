SORTREDUCE_INCLUDE=../../include/
SORTREDUCE_LIB=../../obj/

obj/multi: multi.cpp $(SORTREDUCE_LIB)/libsortreduce.a
	mkdir -p obj
	g++ -o ./obj/multi multi.cpp -Wall -I$(SORTREDUCE_INCLUDE) $(SORTREDUCE_LIB)/libsortreduce.a -std=c++11 -pthread -laio -O2 -march=native
