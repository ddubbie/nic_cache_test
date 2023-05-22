#ifndef __RNG_H__
#define __RNG_H__

#include <stdint.h>

//void rng_setup_zipf(const long num_elements, const double theta);
#ifdef _DEBUG_RNG
int rng_zipf(const double alpha, const int n, double *prob);
#else
int rng_zipf(const double alpha, const int n);
#endif

#ifdef _DEBUG_RNG
uint64_t rng_gev(const double mu, const double sigma, const double xi, double *prob);
uint64_t rng_gpd(const double mu, const double sigma, const double xi, double *prob);
//uint64_t rng_zipfian(double *prob);
#else
uint64_t rng_gev(const double mu, const double sigma, const double xi);
uint64_t rng_gpd(const double mu, const double sigma, const double xi);
//uint64_t rng_zipfian(void);
#endif
uint64_t rng_int32(void);
#endif
