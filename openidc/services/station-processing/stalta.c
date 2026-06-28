/*
 * stalta.c — recursive STA/LTA detector kernel.
 *
 * STA/LTA (Short-Term Average / Long-Term Average) is the classic first stage
 * of automatic seismic station processing: it tracks the ratio between recent
 * signal energy (STA) and background energy (LTA). When a transient (a phase
 * arrival) appears, the ratio spikes and we declare a detection.
 *
 * This is the recursive formulation (as used in Earthworm / ObsPy's
 * recursive_sta_lta): O(n), single pass, constant memory — suitable for
 * continuous real-time station streams.
 *
 * Built into libstalta.so and called from Python via ctypes. The Python module
 * (stalta.py) carries a pure-NumPy fallback so the pipeline still runs if the
 * shared library is unavailable.
 *
 *   cc -O2 -fPIC -shared -o libstalta.so stalta.c
 */
#include <stddef.h>

/*
 * Compute the STA/LTA characteristic function in-place into `out`.
 *
 *   data  : input samples (length n)
 *   n     : number of samples
 *   nsta  : short-term window length in samples (must be >= 1)
 *   nlta  : long-term  window length in samples (must be > nsta)
 *   out   : output characteristic function (length n), caller-allocated
 */
void recursive_sta_lta(const double *data, size_t n,
                       size_t nsta, size_t nlta, double *out)
{
    if (n == 0 || nsta == 0 || nlta == 0) {
        return;
    }

    const double csta = 1.0 / (double) nsta;
    const double clta = 1.0 / (double) nlta;

    double sta = 0.0;
    double lta = 1e-12;   /* tiny non-zero seed to avoid divide-by-zero */

    for (size_t i = 0; i < n; ++i) {
        const double sq = data[i] * data[i];
        sta = csta * sq + (1.0 - csta) * sta;
        lta = clta * sq + (1.0 - clta) * lta;

        /* The LTA needs `nlta` samples to stabilise; suppress early output. */
        out[i] = (i < nlta) ? 0.0 : (sta / lta);
    }
}
