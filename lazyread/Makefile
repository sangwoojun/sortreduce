CC=g++

SRCDIR=./src
INCDIR=./include

CPPFILES=$(wildcard $(SRCDIR)/*.cpp)
INCFILES=$(wildcard $(INCDIR)/*.hpp)

CFLAGS=-static -Wall -pedantic -lpthread -laio --std=c++11 -pthread -I$(INCDIR) -O2 -g

OUTPUTDIR=./obj
OBJ=$(patsubst $(SRCDIR)%,$(OUTPUTDIR)%, $(CPPFILES:.cpp=.o))

.PHONY: clean


$(OUTPUTDIR)/liblazyread.a : $(OBJ)
	ar rcs $(OUTPUTDIR)/liblazyread.a $(OBJ)

$(OUTPUTDIR)/%.o : $(SRCDIR)/%.cpp $(INCDIR)/%.hpp | $(OUTPUTDIR)
	$(CC) -c -o $@ $< $(CFLAGS)

$(OUTPUTDIR):
	mkdir -p $(OUTPUTDIR)

clean:
	rm -rf $(OUTPUTDIR)
