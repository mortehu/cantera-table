#ifndef STORAGE_CA_TABLE_CRC32C_H_
#define STORAGE_CA_TABLE_CRC32C_H_ 1

#include <stddef.h>
#include <stdint.h>

uint32_t ca_crc32c(uint32_t input_crc32, const void* input_buffer,
                   size_t length);

#endif  // !STORAGE_CA_TABLE_CRC32C_H_
