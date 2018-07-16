# Sort-Reduce

Software library for the sort-reduce algorithm

## Introduction

## Building the library

## Usage

```
SortReduceTypes::Config<uint64_t,uint32_t>* conf = new SortReduceTypes::Config<uint64_t,uint32_t>("/mnt/ssd0/scratch/", "output.dat", 16);
conf->SetUpdateFunction(&update_function);
conf->SetManagedBufferSize(1024*1024*32, 64); // 4 GB

SortReduce<uint64_t,uint32_t>* sr = new SortReduce<uint64_t,uint32_t>(conf);
```

```
uint32_t update_function(uint32_t a, uint32_t b) {
	return a+b;
}
```


```
for ( uint64_t i = 0; i < element_count; i++ ) { 
	while ( !sr->Update(key, 1) )}
}
sr->Finish();

```


### Building

### User-defined update function

### Memory allocation

### Thread allocation

### Multi-thread ingestion

### More types

## Implementation details
