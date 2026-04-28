/* rxshare — fast RandomX share verification for the Pricoin pool.
 *
 * Wraps tevador/RandomX in FULL_MEM (hardware) mode so each share hash is
 * a few microseconds instead of milliseconds. Maintains a current dataset
 * and an optional pending dataset built in the background, so epoch
 * rotation never pauses the verifier.
 *
 * The cache-key transform applied to seed_hash matches pricoind's
 * SeedKeyFromHash() in src/pow/randomx_pricoin.cpp byte-for-byte:
 *     cache_key = SHA-256("pricoin/randomx/cache-key/v1" || seed_hash)
 * so a hash computed here equals the chain's GetPoWHashOfBytes for the
 * same input + seed. Callers pass the *raw* seed_hash, not the tagged
 * digest.
 */
#ifndef RXSHARE_H
#define RXSHARE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rxshare_handle rxshare_handle;

#define RXSHARE_OK              0
#define RXSHARE_ERR_NULL       -1
#define RXSHARE_ERR_ALLOC      -2
#define RXSHARE_ERR_INIT       -3
#define RXSHARE_ERR_BUSY       -4   /* prepare called while build in progress */
#define RXSHARE_ERR_NOT_READY  -5   /* swap called with no pending build */
#define RXSHARE_ERR_NO_SWAP    -6   /* swap not supported in light mode */

/* Create a handle and initialize the current dataset for `seed_hash`.
 * `init_threads` controls parallelism for the dataset init (~30 s on 8
 * threads on a fast x86 box; light mode is sub-second).
 * `full_mem`: 1 = ~2.08 GB dataset, fast hashing (production); 0 = ~256
 * MB cache (light) — convenient for tests on small machines but ~10×
 * slower per hash and **not supported** by rxshare_prepare/swap.
 *
 * Returns NULL on allocation/init failure. Errno-style detail is not
 * provided; callers can fall back to light mode and surface a config hint.
 */
rxshare_handle *rxshare_create(const uint8_t seed_hash[32],
                               int init_threads,
                               int full_mem);

/* Compute the RandomX hash of `input_len` bytes at `input` against the
 * current dataset. Writes 32 bytes to `out`. Internally serializes
 * concurrent calls + any swap-in-progress on a single mutex; if you need
 * higher throughput later, create multiple handles with the same seed.
 */
int rxshare_hash(rxshare_handle *h,
                 const uint8_t *input, size_t input_len,
                 uint8_t out[32]);

/* Begin building the next-epoch dataset for `next_seed_hash` in a
 * background thread. Non-blocking. Returns RXSHARE_ERR_BUSY if a build is
 * already pending. Only valid in full_mem mode.
 */
int rxshare_prepare(rxshare_handle *h,
                    const uint8_t next_seed_hash[32],
                    int init_threads);

/* Wait for the pending build to complete, then atomically promote it to
 * the current dataset and free the old one. Returns RXSHARE_OK on success;
 * RXSHARE_ERR_NOT_READY if no prepare was scheduled; RXSHARE_ERR_INIT if
 * the build itself failed.
 */
int rxshare_swap(rxshare_handle *h);

/* Free everything. Safe with NULL. Joins any outstanding build thread. */
void rxshare_destroy(rxshare_handle *h);

#ifdef __cplusplus
}
#endif
#endif /* RXSHARE_H */
