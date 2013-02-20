#ifndef CA_INTERNAL_H_
#define CA_INTERNAL_H_ 1

#ifdef __cplusplus
extern "C" {
#endif

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
