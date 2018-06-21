#include <stdio.h>
#include <stdlib.h>

#include "sortreduce.h"

int main(int argc, char** argv) {
	SortReduce::Config* conf = new SortReduce::Config(SortReduce::KEY_BINARY64, SortReduce::VAL_BINARY32, 0, 0, "./");
	SortReduce* sr = new SortReduce(conf);
}
