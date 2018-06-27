#ifndef __TYPES_H__
#define __TYPES_H__

namespace SortReduceTypes {
	typedef enum {
		KEY_BINARY32,
		KEY_BINARY64,
	} KeyType;
	typedef enum {
		VAL_BINARY32,
		VAL_BINARY64,
	} ValType;

	typedef struct {
		void* buffer;
		size_t bytes;
	} Block;

	typedef struct {
		int fd;
		//size_t bytes;
	} File;
}

#endif
