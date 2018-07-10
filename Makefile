CC=g++


SRCDIR=./src
CPPFILES=$(wildcard $(SRCDIR)/*.cpp)
INCDIR=./include
INCFILES=$(wildcard $(INCDIR)/*.h)

CFLAGS=-static -Wall -pedantic -lpthread -laio --std=c++11 -pthread -I$(INCDIR) -O3

OUTPUTDIR=./obj
OBJ=$(patsubst $(SRCDIR)%,$(OUTPUTDIR)%, $(CPPFILES:.cpp=.o))

.PHONY: clean


$(OUTPUTDIR)/libsortreduce.a : $(OBJ)
	ar rcs $(OUTPUTDIR)/libsortreduce.a $(OBJ)

$(OUTPUTDIR)/%.o : $(SRCDIR)/%.cpp $(INCDIR)/%.h | $(OUTPUTDIR)
	$(CC) -c -o $@ $< $(CFLAGS)

$(OUTPUTDIR):
	mkdir -p $(OUTPUTDIR)

clean:
	rm -r $(OUTPUTDIR)
