CC=g++


SORTREDUCE_INCLUDE=../../include/
SORTREDUCE_LIB=../../obj/
HELPER_DIR=../../helpers/
CPPFILES=$(wildcard ./*.cpp) $(wildcard $(HELPER_DIR)/*.cpp)

obj/kmercount: kmercount.cpp $(SORTREDUCE_LIB)/libsortreduce.a
	mkdir -p obj
	$(CC) -o ./obj/kmercount $(CPPFILES) -Wall -I$(SORTREDUCE_INCLUDE) -I$(HELPER_DIR) $(SORTREDUCE_LIB)/libsortreduce.a -std=c++11 -pthread -laio
