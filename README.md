# Sort-Reduce

Software library for the sort-reduce algorithm.

## Introduction to sort-reduce

Sort-reduce is an algorithm for sequentializing fine-grained random read-modify-writes, or updates, into secondary storage.

Because most secondary storage devices have coarse, multi-KB, page level access granularities, updating fine-grained values in secondary storage incur a large write amplification.
For example, reading and writing an 8 KB page in order to read and update a 4 byte value results in a 2048-fold write amplification, resulting in a 2048-fold effective performance degradation.

Sort-reduce is an algorithm that mitigates this issue in two parts:
- Log the update requests and sorting it, turning the requests sequential.
- During sorting, whenever update requests directed to the same location are discovered, apply the update between the two requests.

Random updates that benefit from sort-reduce can be formulated like the following:
Updating an array **X** in secondary storage using an update function **f** would look like the following:
For each update request into __index__ using __argument__, X[index] = **f**(X[index], argument).
For example, constructing a histogram would repeat X[index] = X[index] + 1.

Sort-reduce logs and sort the <__index__,__argument__> pair for all update requests, turning it sequential.
In order to reduce the overhead of sorting, whenever two update requests with the same __index__ are discovered, **f** is applied between two __argument__, reducing two update requests into one.

More details about the sort-reduce algorithm, as well as its application to graph analytics, can be seen in the paper [GraFBoost: Accelerated Flash Storage for External Graph Analytics](http://people.csail.mit.edu/wjun/papers/isca2018-camera.pdf).


## Building the library

The library can be either built into a .a file or be used by building and linking its .cpp file.

libsortreduce.a can be built simply by running **make**


### Key-Value types in the library

The key and value types the library can handle can be specified in the Makefile.

Compiler flags **KTYPES1**,  **KTYPES2**,  **KTYPES3**, and **KTYPES4** can be used to specify which types the library can handle.
See the example flags set in the default Makefile.


## Usage

The library is templatized with the array index (key) and argument (value) types.
For example, for 64-bit keys and 32-bit values, the library can be instantiated like the following:

```
SortReduceTypes::Config<uint64_t,uint32_t>* conf = new SortReduceTypes::Config<uint64_t,uint32_t>("/mnt/ssd0/scratch/", "output.dat", 16);
conf->SetUpdateFunction(&update_function);


SortReduce<uint64_t,uint32_t>* sr = new SortReduce<uint64_t,uint32_t>(conf);
```

The above code uses "/mnt/ssd0/scratch/" for temporary and output directories, allows up to 16 threads to be used, and stores the sort-reduced array at "/mnt/ssd0/scratch/output.dat".
The user-defined update function is given as update_function.


```
SortReduceTypes::Config<uint64_t,uint32_t>* conf = new SortReduceTypes::Config<uint64_t,uint32_t>("/mnt/ssd0/scratch/", "", 16);
```

An example update function for building histograms can be seen below.

```
uint32_t update_function(uint32_t a, uint32_t b) {
	return a+b;
}
```

Update requests can be applied like as shown below.
Update may return false if there is no buffer space, so we must loop until it doesn't.
Once all input is applied, call Finish().

```
for ( uint64_t i = 0; i < element_count; i++ ) { 
	while ( !sr->Update(key, 1) ){}
}
sr->Finish();

```

After all input is given, we must wait until sort-reduce is done, like the following:
Hundreds of gigabytes worth of updates, sort-reduce may take minutes to finish.

```
SortReduceTypes::Status status = sr->CheckStatus();
while ( status.done_external == false ) {
	sleep(1);
	status = sr->CheckStatus();
}
```

Once sort-reduce finishes, the sort-reduced results can be read like the following:

```
std::tuple<uint64_t,uint32_t,bool> kvp = sr->Next();
while ( std::get<2>(kvp) ) {
	uint64_t key = std::get<0>(kvp);
	uint32_t val = std::get<1>(kvp);
	kvp = sr->Next();
}
```


### Building user software

See example makefiles in examples/simple

### Multi-thread ingestion

See example makefiles in examples/multi

## Implementation details
