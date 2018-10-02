CC=g++

SRCDIR=./src
CPPFILES=$(wildcard $(SRCDIR)/*.cpp)
INCDIR=./include
INCFILES=$(wildcard $(INCDIR)/*.h)

CFLAGS=-static -Wall -pedantic -lpthread -laio --std=c++11 -pthread -I$(INCDIR) -march=native -O2
SRFLAGS=-DKVTYPES1='uint64_t,uint32_t'  -DKVTYPES2='uint32_t,uint32_t'

OUTPUTDIR=./obj
OBJ=$(patsubst $(SRCDIR)%,$(OUTPUTDIR)%, $(CPPFILES:.cpp=.o))

.PHONY: clean


$(OUTPUTDIR)/libsortreduce.a : $(OBJ)
	ar rcs $(OUTPUTDIR)/libsortreduce.a $(OBJ)

$(OUTPUTDIR)/%.o : $(SRCDIR)/%.cpp $(INCDIR)/%.h | $(OUTPUTDIR)
	$(CC) -c -o $@ $< $(CFLAGS) $(SRFLAGS)

$(OUTPUTDIR):
	mkdir -p $(OUTPUTDIR)

clean:
	rm -r $(OUTPUTDIR)
