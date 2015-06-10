#ifndef STORAGE_CA_TABLE_RLE_H_
#define STORAGE_CA_TABLE_RLE_H_ 1

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct CA_rle_context {
  uint8_t* data;
  uint8_t value, run;
};

void CA_rle_init_write(struct CA_rle_context* ctx, uint8_t* output);

uint8_t* CA_rle_flush(struct CA_rle_context* ctx);

void CA_rle_put(struct CA_rle_context* ctx, uint8_t value);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // !STORAGE_CA_TABLE_RLE_H_
