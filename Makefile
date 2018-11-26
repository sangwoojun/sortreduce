CC=g++

SRCDIR=./src
INCDIR=./include

# comment out if not using FPGA
FPGADIR=../bluespecpcie/cpp
FPGADRAMDIR=../bluespecpcie/dram/cpp
FPGAFLAG=


CPPFILES=$(wildcard $(SRCDIR)/*.cpp) $(wildcard $(FPGADIR)/*.cpp)
INCFILES=$(wildcard $(INCDIR)/*.h) $(wildcard $(FPGADIR)/*.h)

CFLAGS=-static -Wall -pedantic -lpthread -laio --std=c++11 -pthread -I$(INCDIR) -I$(FPGADIR) $(FPGAFLAG) -march=native -O2 -g
#-DHW_ACCEL
SRFLAGS=-DKVTYPES1='uint64_t,uint32_t'  -DKVTYPES2='uint32_t,uint32_t' -DKVTYPES3='uint32_t,uint64_t'

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
	rm -rf $(OUTPUTDIR)
