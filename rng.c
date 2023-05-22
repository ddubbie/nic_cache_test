#include "rng.h"
#include "mt19937ar.h"
#include "genzipf.h"
#include <math.h>
#include <time.h>
#include <assert.h>

/* TODO
 * Generalized Extreme Value Distribution
 *
 * GEV(mu, sigma, k)
 *
 * Generalized Pareto
 * GPD(theta, sigmak, k)
 *
 * https://cse.usf.edu/~kchriste/tools/genzipf.c
 * */
static void InitRng(void) __attribute__((constructor));
#if 0
static long _n_elements = 0;
static double _theta = 0;
static double _alpha;
static double _zetan;
static double _zeta2theta;
static double _eta;

static double zeta(const uint64_t num_elements, const double theta);

static double
zeta(const uint64_t num_elements, double theta) {

    double sum = 0;

    for (uint64_t i = 0; i < num_elements; i++) {
        sum += 1.0 / (pow(i+1, theta));
    }
    
    return sum;
}

static void
InitRng(void) {

    init_genrand(time(NULL));
}

void 
rng_setup_zipf(const long num_elements, const double theta) {
    
    _n_elements = num_elements;
    _theta = theta;

    _alpha = 1.0 / (1.0 - theta);
    _zetan = zeta(num_elements, theta);
    _eta = (1 - pow(2.0 / num_elements, 1 - theta)) / (1 - zeta(theta, 2) / _zetan);
}
#endif

uint64_t rng_int32(void) {
    return genrand_int32();
}

static void
InitRng(void) {

    init_genrand(time(NULL));
}

uint64_t 
#ifdef _DEBUG_RNG
rng_gev(const double mu, const double sigma, const double xi, double *prob) {
#else /* _DEBUG_RNG */
rng_gev(const double mu, const double sigma, const double xi) {
#endif /*_DEBUG_RNG */

    double p = genrand_real1();
#ifdef _DEBUG_RNG
    *prob = p;
#endif
    if (xi == 0) {
        return (uint64_t)(mu - sigma * log(-log(p)));
    } else {
        return (uint64_t)(mu + (sigma / xi) * (pow(-log(p), -xi) - 1));
    }
}

#ifdef _DEBUG_RNG
int
rng_zipf(const double alpha, const int n, double *prob) {
    return zipf(alpha, n, prob);
}
#else
int
rng_zipf(const double alpha, const int n) {
    return zipf(alpha, n);
}
#endif

uint64_t 
#ifdef _DEBUG_RNG
rng_gpd(const double mu, const double sigma, const double xi, double *prob) {
#else
rng_gpd(const double mu, const double sigma, const double xi) {
#endif

    double p = genrand_real1();
#ifdef _DEBUG_RNG
    *prob = p;
#endif
    return (uint64_t)((sigma / xi) * (pow(1-p, -xi) - 1) + mu);
}

#if 0

uint64_t
#ifdef _DEBUG_RNG
rng_zipfian(double *prob) {
#else
rng_zipfian(void) {
#endif

    double u = genrand_real1();
    double uz = u * _zetan;

#ifdef _DEBUG_RNG
    *prob = u;
#endif

    if (uz < 1.0) {
        return 1;
    } else if (uz < 1.0 + pow(0.5, _theta)) {
        return 2;
    } 

    return 1 + (long)(_n_elements * pow(_eta * u - _eta + 1, _alpha));
}
#endif

