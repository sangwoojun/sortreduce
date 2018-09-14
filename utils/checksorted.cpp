#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

int
main (int argc, char** argv) {
	if ( argc < 4 ) {
		printf( "usage: %s filename keybytes valbytes\n", argv[0] );
		exit(1);
	}

	FILE* fin = fopen(argv[1], "rb");

	int keybytes = atoi(argv[2]);
	int valbytes = atoi(argv[3]);

	if ( keybytes > 8 ) {
		printf( "Tool only supports 4 or 8 byte keys\n" );
		exit(1);
	}
	
	fseek(fin, SEEK_SET, 0);
	uint64_t byte_off = 0;
	uint64_t last_key = 0;
	while (!feof(fin)) {
		uint64_t key = 0;
		int r = fread(&key, keybytes, 1, fin);
		if ( r != 1 ) break;

		fseek(fin, valbytes, SEEK_CUR);
		
		if ( last_key > key ) {
			printf( "%lx : %lx > %lx !!\n", byte_off, last_key, key );
		}

		last_key = key;
		byte_off += keybytes + valbytes;
	}
	
	return 0;
}
