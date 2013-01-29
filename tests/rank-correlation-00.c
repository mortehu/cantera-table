#include <stdio.h>
#include <stdlib.h>

#include "ca-functions.h"

struct
{
  float values[4];
  float correlation;
} test_cases[] =
{
    { { 0.0, 1.0, 2.0, 3.0 },  1.0 },
    { { 3.0, 2.0, 1.0, 0.0 }, -1.0 },
    { { 1.0, 0.0, 0.0, 1.0 },  0.0 }
};

int
main (int argc, char **argv)
{
  size_t i;
  int result = EXIT_SUCCESS;

  for (i = 0; i < sizeof (test_cases) / sizeof (test_cases[0]); ++i)
    {
      float correlation;

      correlation = ca_stats_rank_correlation (test_cases[i].values, 4);

      if (correlation != test_cases[i].correlation)
        {
          fprintf (stderr, "Test case %zu: Got correlation %.7g, expected %.7g\n",
                   i, correlation, test_cases[i].correlation);

          result = EXIT_FAILURE;
        }
    }

  return result;
}
