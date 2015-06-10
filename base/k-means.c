#include <assert.h>
#include <string.h>
#include <stdlib.h>

#include "base/k-means.h"

#define POW2(v) ((v) * (v))

struct Kmeans*
kmeans_create (float *const *data, size_t sample_count,
               size_t dimensions, size_t k)
{
  struct Kmeans *result;

  result = calloc (1, sizeof (*result));
  result->data = data;
  result->sample_count = sample_count;
  result->dimensions = dimensions;
  result->k = k;

  result->indexes = calloc (sample_count, sizeof (*result->indexes));
  result->centers = calloc (k * dimensions, sizeof (*result->centers));

#ifndef NDEBUG
  size_t i;

  for (i = 0; i < sample_count; ++i)
    assert (data[i]);
#endif

  return result;
}

void
kmeans_free (struct Kmeans *kmeans)
{
  free (kmeans->centers);
  free (kmeans->indexes);
  free (kmeans);
}

void
kmeans_randomize_centers(struct Kmeans *kmeans, enum kmeans_randomize_mode mode)
{
  size_t remaining, dim;

  dim = kmeans->dimensions;
  remaining = kmeans->sample_count;

  switch (mode)
    {
    case kmeans_random:

        {
          size_t m, i = 0, o = 0;

          m = (kmeans->k < remaining) ? kmeans->k : remaining;

          while (m > 0)
            {
              if ((rand() % remaining) < m)
                {
                  memcpy (kmeans->centers + o * dim,
                          kmeans->data[i],
                          dim * sizeof (*kmeans->centers));

                  ++o;
                  --m;
                }

              --remaining;
              ++i;
            }
        }

      break;

    case kmeans_plusplus:

        {
          size_t i, j;
          float *distances, r;

          i = rand () % kmeans->sample_count;

          memcpy (kmeans->centers,
                  kmeans->data[i],
                  dim * sizeof (*kmeans->centers));

          distances = calloc (sizeof (*distances), kmeans->sample_count);

          for (i = 1; i < kmeans->k; ++i)
            {
              fprintf (stderr, "\rFinding center %zu / %zu", i + 1, kmeans->k);

              kmeans_calculate_indexes (kmeans, i, distances);

              for (j = 1; j < kmeans->sample_count; ++j)
                distances[j] += distances[j - 1];

              r = distances[kmeans->sample_count - 1] * rand () / RAND_MAX;

              for (j = 0; j < kmeans->sample_count; ++j)
                {
                  if (distances[j] >= r)
                    break;
                }

              memcpy (kmeans->centers + i * dim,
                      kmeans->data[j],
                      dim * sizeof (*kmeans->centers));
            }

          fprintf (stderr, "\n");

          free (distances);
        }
      break;
    }
}

void
kmeans_print(struct Kmeans* kmeans)
{
  size_t i, j;
  static size_t offset;

#if 0
  printf("Number of groups: %zu\n", kmeans->k);
  printf("Centers:\n");

#endif
  for (i = 0; i < kmeans->k; ++i)
    {
#if 0
      if (i)
        putchar (' ');
#endif

      for (j = 0; j < kmeans->dimensions; ++j)
        printf("%zu %.6g\n", offset + j, kmeans->centers[i * kmeans->dimensions + j]);

      printf("\n");
    }

  offset += kmeans->dimensions;
#if 0

  printf("Index:\n");

  for (i = 0; i < kmeans->sample_count; ++i)
    {
      if (i)
        putchar (' ');

      printf("%u", kmeans->indexes[i]);
    }

  printf ("\n");
#endif
}

void
kmeans_recalculate_centers(struct Kmeans *kmeans)
{
  size_t i, j, dim;
  size_t *counts;
  float *center;

  counts = calloc (kmeans->k, sizeof (*counts));

  dim = kmeans->dimensions;

  memset (kmeans->centers,
          0,
          sizeof (*kmeans->centers) * dim * kmeans->k);

  for (i = 0; i < kmeans->sample_count; ++i)
    {
      ++counts[kmeans->indexes[i]];

      center = kmeans->centers + kmeans->indexes[i] * dim;

      for (j = 0; j < dim; ++j)
        center[j] += kmeans->data[i][j];
    }

  for (i = 0; i < kmeans->k; ++i)
    {
      float scale;

      center = kmeans->centers + i * dim;

      if (counts[i] <= 1)
        continue;

      scale = 1.0 / counts[i];

      for (j = 0; j < dim; ++j)
        center[j] *= scale;
    }

  free (counts);
}

static float
KMEANS_square_distance(const float *lhs, const float *rhs, size_t n)
{
  float result = 0;

  while (n--)
    {
      result += (*lhs - *rhs) * (*lhs - *rhs);

      ++lhs, ++rhs;
    }

  return result;
}

double
kmeans_calculate_indexes(struct Kmeans *kmeans, size_t k, float *distances)
{
  size_t i, j, old_index, dim;
  const float *sample, *center;
  float *center_distances, *max_distances;
  double result = 0.0;

  dim = kmeans->dimensions;

#define IDX(i, j) ((i) * (k) - (i) * ((i) + 1) / 2 + ((j) - (i) - 1))

  center_distances = calloc (IDX(k, k + 1), sizeof (*center_distances));
  max_distances = calloc (k, sizeof (*max_distances));
  kmeans->dirty = 0;

  for (i = 0; i < k; ++i)
    max_distances[i] = HUGE_VAL;

  for (i = 0; i < k; ++i)
    {
      center = kmeans->centers + i * dim;

      for (j = i + 1; j < k; ++j)
        {
          float *center_b, distance;

          center_b = kmeans->centers + j * dim;

          assert (IDX (i, j) < IDX (k, k + 1));

          distance = sqrt (KMEANS_square_distance (center, center_b, dim));

          center_distances[IDX(i, j)] = distance;

          if (distance < max_distances[i])
            max_distances[i] = distance;

          if (distance < max_distances[j])
            max_distances[j] = distance;
        }
    }

  for (i = 0; i < k; ++i)
    max_distances[i] *= 0.5;

  for (i = 0; i < kmeans->sample_count; ++i)
    {
      float min_sq_distance, min_distance;

      sample = kmeans->data[i];

      old_index = kmeans->indexes[i];
      center = kmeans->centers + old_index * dim;

      min_sq_distance = KMEANS_square_distance (sample, center, dim);
      min_distance = sqrt (min_sq_distance);

      if (min_distance > max_distances[old_index])
        {
          for (j = 0; j < k; ++j)
            {
              float distance, center_distance;

              if (old_index < j)
                center_distance = center_distances[IDX (old_index, j)];
              else if (old_index > j)
                center_distance = center_distances[IDX (j, old_index)];
              else
                continue;

              if (center_distance - min_distance > min_distance)
                continue;

              center = kmeans->centers + j * dim;

              distance = KMEANS_square_distance (sample, center, dim);

              if (distance < min_sq_distance)
                {
                  min_sq_distance = distance;
                  min_distance = sqrt (min_sq_distance);
                  old_index = j;
                  kmeans->indexes[i] = j;
                  kmeans->dirty = 1;
                }
            }
        }

      result += min_sq_distance;

      if (distances)
        distances[i] = min_sq_distance;
    }

  free (max_distances);
  free (center_distances);

#undef IDX

  return result;
}

float
kmeans_cluster(struct Kmeans *kmeans, size_t maxiter)
{
  double error = 0.0;

  while (maxiter--)
    {
      error = kmeans_calculate_indexes (kmeans, kmeans->k, NULL);

      if (!kmeans->dirty)
        break;

      kmeans_recalculate_centers (kmeans);
    }

  return error;
}
