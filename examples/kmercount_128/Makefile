SORTREDUCE_INCLUDE=../../include/
SORTREDUCE_LIB=../../obj/

obj/kmercount: kmercount.cpp $(SORTREDUCE_LIB)/libsortreduce.a
	mkdir -p obj
	g++ -o ./obj/kmercount kmercount.cpp -Wall -I$(SORTREDUCE_INCLUDE) -L$(SORTREDUCE_LIB) -std=c++11 -lsortreduce -pthread -laio -march=native -lrt -g

clean:
	rm -rf obj
	rm -rf output
	rm -rf out.sr