#include <stdint.h>

#include "ca-internal.h"

void
CA_rle_init_write (struct CA_rle_context *ctx, uint8_t *output)
{
  ctx->data = output;
  ctx->value = 0;
  ctx->run = 0;
}

void
CA_rle_init_read (struct CA_rle_context *ctx, const uint8_t *input)
{
  ctx->data = (uint8_t *) input;
  ctx->value = 0;
  ctx->run = 0;
}

uint8_t *
CA_rle_flush (struct CA_rle_context *ctx)
{
  if (!ctx->run)
    return ctx->data;

  if (ctx->run <= 2 && ctx->value < 0xc0)
    {
      do
        {
          ctx->data[0] = ctx->value;
          ctx->data += 1;
        }
      while (--ctx->run);
    }
  else
    {
      ctx->data[0] = 0xc0 | (ctx->run - 1);
      ctx->data[1] = ctx->value;
      ctx->data += 2;

      ctx->run = 0;
    }

  return ctx->data;
}

uint8_t
CA_rle_get (struct CA_rle_context *ctx)
{
  if (ctx->run)
    {
      --ctx->run;

      return ctx->value;
    }

  if (0xc0 == (0xc0 & ctx->data[0]))
    {
      ctx->run = ctx->data[0] & 0x3f;
      ctx->value = ctx->data[1];
      ctx->data += 2;
    }
  else
    ctx->data += 1;

  return ctx->data[-1];
}

void
CA_rle_put (struct CA_rle_context *ctx, uint8_t value)
{
  if (value != ctx->value && ctx->run)
    CA_rle_flush (ctx);

  ctx->value = value;

  if (++ctx->run == 0x40)
    {
      ctx->data[0] = 0xff;
      ctx->data[1] = ctx->value;
      ctx->data += 2;

      ctx->run = 0;
    }
}
