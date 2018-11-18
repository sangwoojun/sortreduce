SORTREDUCE_INCLUDE=../../include/
SORTREDUCE_LIB=../../obj/

GRAPH_SRC=src/
GRAPH_CPP=$(wildcard $(GRAPH_SRC)/*.cpp)

SRFLAGS=-DKVTYPES1='uint64_t,uint32_t'  -DKVTYPES2='uint32_t,uint32_t'

obj/graph: graph.cpp $(SORTREDUCE_LIB)/libsortreduce.a $(GRAPH_CPP)
	mkdir -p obj
	g++ -o ./obj/graph graph.cpp $(GRAPH_CPP) $(SRFLAGS) -Wall -I$(SORTREDUCE_INCLUDE) -I$(GRAPH_SRC) -L$(SORTREDUCE_LIB) -std=c++11 -pedantic -lsortreduce -pthread -laio -march=native -lrt -g
