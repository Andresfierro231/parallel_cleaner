#include <math.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>

static void clean_spikes(const double *x, double *y, int *flags, int n, int window, double zthr) {
  #pragma omp parallel for schedule(static)
  for (int i = 0; i < n; ++i) {
    int left = i - window < 0 ? 0 : i - window;
    int right = i + window + 1 > n ? n : i + window + 1;
    double sum = 0.0;
    int count = 0;
    for (int j = left; j < right; ++j) {
      if (j == i) continue;
      sum += x[j];
      count++;
    }
    if (count < 2) {
      flags[i] = 0;
      y[i] = x[i];
      continue;
    }
    double mean = sum / count;
    double var = 0.0;
    for (int j = left; j < right; ++j) {
      if (j == i) continue;
      double d = x[j] - mean;
      var += d * d;
    }
    double std = sqrt(var / (count - 1));
    double dev = fabs(x[i] - mean);
    flags[i] = (std > 0.0 && dev > zthr * std) ? 1 : 0;
    y[i] = flags[i] ? mean : x[i];
  }
}

int main(int argc, char **argv) {
  int n = 1000000;
  int window = 5;
  double zthr = 3.0;
  if (argc > 1) n = atoi(argv[1]);
  double *x = (double*) malloc((size_t)n * sizeof(double));
  double *y = (double*) malloc((size_t)n * sizeof(double));
  int *flags = (int*) malloc((size_t)n * sizeof(int));
  if (!x || !y || !flags) {
    fprintf(stderr, "allocation failure\n");
    return 1;
  }

  for (int i = 0; i < n; ++i) {
    x[i] = sin(0.01 * i);
  }
  for (int i = 1000; i < n; i += 50000) {
    x[i] += 10.0;
  }

  double t0 = omp_get_wtime();
  clean_spikes(x, y, flags, n, window, zthr);
  double t1 = omp_get_wtime();

  int flagged = 0;
  for (int i = 0; i < n; ++i) flagged += flags[i];
  printf("n=%d threads=%d flagged=%d elapsed_sec=%.6f\n", n, omp_get_max_threads(), flagged, t1 - t0);

  free(x);
  free(y);
  free(flags);
  return 0;
}
