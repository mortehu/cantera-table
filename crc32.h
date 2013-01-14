#ifndef CRC32_H_
#define CRC32_H_ 1

#include <stdlib.h>
#include <stdint.h>

uint32_t
crc32(uint32_t input_crc32, const void *input_buffer, size_t length);

#endif /* !CRC32_H_ */
