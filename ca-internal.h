#ifndef CA_INTERNAL_H_
#define CA_INTERNAL_H_ 1

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

struct CA_rle_context
{
  uint8_t *data;
  uint8_t value, run;
};

void
CA_rle_init_write (struct CA_rle_context *ctx, uint8_t *output);

void
CA_rle_init_read (struct CA_rle_context *ctx, const uint8_t *input);

uint8_t *
CA_rle_flush (struct CA_rle_context *ctx);

void
CA_rle_put (struct CA_rle_context *ctx, uint8_t value);

uint8_t
CA_rle_get (struct CA_rle_context *ctx);

struct CA_float_rank
{
  float value;
  unsigned int rank;
};

void
CA_sort_float_rank (struct CA_float_rank *data, size_t count);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !CA_INTERNAL_H_ */
