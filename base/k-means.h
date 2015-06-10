#include <stdio.h>
#include <math.h>

#ifdef __cplusplus
extern "C" {
#endif

struct Kmeans {
  float* const* data;
  size_t sample_count;
  size_t dimensions;
  size_t k;

  unsigned int* indexes;
  float* centers;
  int dirty;
};

struct Kmeans* kmeans_create(float* const* data, size_t sample_count,
                             size_t dimensions, size_t k);

void kmeans_free(struct Kmeans* kmeans);

enum kmeans_randomize_mode { kmeans_random, kmeans_plusplus };

/*
   Randomly chooses centers of 'ngroup' groups.
   These centers are initial and will be changed iteratively.
 */
void kmeans_randomize_centers(struct Kmeans* kmeans,
                              enum kmeans_randomize_mode mode);

/*
   Prints the given Kmeans structure
 */
void kmeans_print(struct Kmeans* kmeans);

/*
   Calculates group centers. Those centers are calculated using iteration
   and they are usually different from the initial centers. Recalculation
   process remains while the last two solutions are not equal.
 */
void kmeans_recalculate_centers(struct Kmeans* kmeans);

/*
   Constructs the index variable. This variable shows the current
   group of the each single observation.
 */
double kmeans_calculate_indexes(struct Kmeans* kmeans, size_t k,
                                float* distances);

/*
   This is the main method of the algorithm.
*/
float kmeans_cluster(struct Kmeans* kmeans, size_t maxiter);

#ifdef __cplusplus
} /* extern "C" */
#endif
