/* rxshare — see rxshare.h for the API contract. */

#include "rxshare.h"
#include <randomx.h>           /* from $(RANDOMX_INC) */

#include <openssl/sha.h>       /* libcrypto: SHA256 */

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/* Tag must match SeedKeyFromHash in src/pow/randomx_pricoin.cpp. */
static const char kCacheKeyTag[] = "pricoin/randomx/cache-key/v1";

static void compute_cache_key(const uint8_t seed_hash[32], uint8_t out[32])
{
    /* Tag is 28 bytes ('pricoin/randomx/cache-key/v1'), seed is 32, so
     * the concatenation fits in 60 bytes — small enough to keep on the
     * stack and call the one-shot SHA256() (the streaming SHA256_*
     * functions are deprecated in OpenSSL 3.0). */
    enum { TAG_LEN = sizeof(kCacheKeyTag) - 1 };
    uint8_t buf[TAG_LEN + 32];
    memcpy(buf,            kCacheKeyTag, TAG_LEN);
    memcpy(buf + TAG_LEN,  seed_hash,    32);
    SHA256(buf, sizeof(buf), out);
}

struct rxshare_handle {
    int full_mem;
    randomx_flags flags;

    /* vm_lock guards `vm` and the current_ds pointer it's bound to.
     * Hashing acquires it; swap also acquires it briefly to swing the
     * VM over to the new dataset. */
    pthread_mutex_t vm_lock;
    randomx_vm     *vm;
    randomx_dataset *current_ds;     /* full_mem only */
    randomx_cache   *current_cache;  /* light mode only */

    /* build_lock guards the pending_* fields plus build_in_progress.
     * The build thread itself runs without holding it (heavy work);
     * we just guard the flag/handover. */
    pthread_mutex_t build_lock;
    pthread_t       build_thread;
    int             build_in_progress;
    int             build_failed;
    int             build_threads;
    randomx_dataset *pending_ds;
};

/* ---------- parallel dataset init ---------- */

struct init_args {
    randomx_dataset *ds;
    randomx_cache *cache;
    unsigned long start;
    unsigned long count;
};

static void *init_worker(void *p)
{
    struct init_args *a = (struct init_args *)p;
    randomx_init_dataset(a->ds, a->cache, a->start, a->count);
    return NULL;
}

static int init_dataset_parallel(randomx_dataset *ds,
                                 randomx_cache *cache,
                                 int n_threads)
{
    if (n_threads < 1) n_threads = 1;
    unsigned long total = randomx_dataset_item_count();
    unsigned long per   = total / (unsigned long)n_threads;
    unsigned long rem   = total % (unsigned long)n_threads;

    pthread_t       *threads = (pthread_t *)calloc((size_t)n_threads, sizeof(*threads));
    struct init_args *args   = (struct init_args *)calloc((size_t)n_threads, sizeof(*args));
    if (!threads || !args) { free(threads); free(args); return RXSHARE_ERR_ALLOC; }

    unsigned long start = 0;
    int spawned = 0;
    int rc = RXSHARE_OK;
    for (int i = 0; i < n_threads; ++i) {
        args[i].ds    = ds;
        args[i].cache = cache;
        args[i].start = start;
        args[i].count = per + ((unsigned long)i < rem ? 1UL : 0UL);
        start += args[i].count;
        if (pthread_create(&threads[i], NULL, init_worker, &args[i]) != 0) {
            rc = RXSHARE_ERR_INIT;
            break;
        }
        ++spawned;
    }
    for (int i = 0; i < spawned; ++i) pthread_join(threads[i], NULL);
    free(threads);
    free(args);
    return rc;
}

/* ---------- background build (full_mem only) ---------- */

struct build_ctx {
    rxshare_handle *h;
    uint8_t seed[32];
    int threads;
};

static void *build_worker(void *p)
{
    struct build_ctx *ctx = (struct build_ctx *)p;
    rxshare_handle *h = ctx->h;

    randomx_dataset *ds = NULL;
    randomx_cache *cache = randomx_alloc_cache(h->flags);
    if (!cache) goto fail;

    uint8_t cache_key[32];
    compute_cache_key(ctx->seed, cache_key);
    randomx_init_cache(cache, cache_key, sizeof(cache_key));

    ds = randomx_alloc_dataset(h->flags);
    if (!ds) goto fail;

    if (init_dataset_parallel(ds, cache, ctx->threads) != RXSHARE_OK) goto fail;

    randomx_release_cache(cache);
    cache = NULL;

    pthread_mutex_lock(&h->build_lock);
    h->pending_ds = ds;
    pthread_mutex_unlock(&h->build_lock);

    free(ctx);
    return NULL;

fail:
    if (ds) randomx_release_dataset(ds);
    if (cache) randomx_release_cache(cache);
    pthread_mutex_lock(&h->build_lock);
    h->build_failed = 1;
    pthread_mutex_unlock(&h->build_lock);
    free(ctx);
    return NULL;
}

/* ---------- public API ---------- */

rxshare_handle *rxshare_create(const uint8_t seed_hash[32],
                               int init_threads,
                               int full_mem)
{
    if (!seed_hash) return NULL;
    if (init_threads < 1) init_threads = 1;

    rxshare_handle *h = (rxshare_handle *)calloc(1, sizeof(*h));
    if (!h) return NULL;
    if (pthread_mutex_init(&h->vm_lock,    NULL) != 0) { free(h); return NULL; }
    if (pthread_mutex_init(&h->build_lock, NULL) != 0) {
        pthread_mutex_destroy(&h->vm_lock);
        free(h);
        return NULL;
    }
    h->full_mem      = full_mem ? 1 : 0;
    h->build_threads = init_threads;
    h->flags         = randomx_get_flags();
    if (h->full_mem) h->flags |= RANDOMX_FLAG_FULL_MEM;

    randomx_cache *boot_cache = randomx_alloc_cache(h->flags);
    if (!boot_cache) goto err;
    uint8_t cache_key[32];
    compute_cache_key(seed_hash, cache_key);
    randomx_init_cache(boot_cache, cache_key, sizeof(cache_key));

    if (h->full_mem) {
        h->current_ds = randomx_alloc_dataset(h->flags);
        if (!h->current_ds) { randomx_release_cache(boot_cache); goto err; }
        if (init_dataset_parallel(h->current_ds, boot_cache, init_threads) != RXSHARE_OK) {
            randomx_release_cache(boot_cache);
            goto err;
        }
        randomx_release_cache(boot_cache);  /* dataset has consumed it */
        h->vm = randomx_create_vm(h->flags, NULL, h->current_ds);
    } else {
        h->current_cache = boot_cache;
        h->vm = randomx_create_vm(h->flags, h->current_cache, NULL);
    }
    if (!h->vm) goto err;

    return h;

err:
    if (h->vm)            randomx_destroy_vm(h->vm);
    if (h->current_ds)    randomx_release_dataset(h->current_ds);
    if (h->current_cache) randomx_release_cache(h->current_cache);
    pthread_mutex_destroy(&h->vm_lock);
    pthread_mutex_destroy(&h->build_lock);
    free(h);
    return NULL;
}

int rxshare_hash(rxshare_handle *h,
                 const uint8_t *input, size_t input_len,
                 uint8_t out[32])
{
    if (!h || !input || !out) return RXSHARE_ERR_NULL;
    pthread_mutex_lock(&h->vm_lock);
    randomx_calculate_hash(h->vm, input, input_len, out);
    pthread_mutex_unlock(&h->vm_lock);
    return RXSHARE_OK;
}

int rxshare_prepare(rxshare_handle *h,
                    const uint8_t next_seed_hash[32],
                    int init_threads)
{
    if (!h || !next_seed_hash) return RXSHARE_ERR_NULL;
    if (!h->full_mem) return RXSHARE_ERR_NO_SWAP;
    if (init_threads < 1) init_threads = h->build_threads;

    pthread_mutex_lock(&h->build_lock);
    if (h->build_in_progress) {
        pthread_mutex_unlock(&h->build_lock);
        return RXSHARE_ERR_BUSY;
    }
    h->build_in_progress = 1;
    h->build_failed      = 0;
    pthread_mutex_unlock(&h->build_lock);

    struct build_ctx *ctx = (struct build_ctx *)malloc(sizeof(*ctx));
    if (!ctx) {
        pthread_mutex_lock(&h->build_lock);
        h->build_in_progress = 0;
        pthread_mutex_unlock(&h->build_lock);
        return RXSHARE_ERR_ALLOC;
    }
    ctx->h       = h;
    ctx->threads = init_threads;
    memcpy(ctx->seed, next_seed_hash, 32);

    if (pthread_create(&h->build_thread, NULL, build_worker, ctx) != 0) {
        free(ctx);
        pthread_mutex_lock(&h->build_lock);
        h->build_in_progress = 0;
        pthread_mutex_unlock(&h->build_lock);
        return RXSHARE_ERR_INIT;
    }
    return RXSHARE_OK;
}

int rxshare_swap(rxshare_handle *h)
{
    if (!h) return RXSHARE_ERR_NULL;
    if (!h->full_mem) return RXSHARE_ERR_NO_SWAP;

    pthread_mutex_lock(&h->build_lock);
    int in_progress = h->build_in_progress;
    pthread_mutex_unlock(&h->build_lock);
    if (!in_progress) return RXSHARE_ERR_NOT_READY;

    pthread_join(h->build_thread, NULL);

    pthread_mutex_lock(&h->build_lock);
    int failed = h->build_failed;
    h->build_in_progress = 0;
    randomx_dataset *new_ds = h->pending_ds;
    h->pending_ds = NULL;
    pthread_mutex_unlock(&h->build_lock);

    if (failed) {
        if (new_ds) randomx_release_dataset(new_ds);
        return RXSHARE_ERR_INIT;
    }
    if (!new_ds) return RXSHARE_ERR_INIT;

    pthread_mutex_lock(&h->vm_lock);
    randomx_dataset *old_ds = h->current_ds;
    h->current_ds = new_ds;
    randomx_vm_set_dataset(h->vm, h->current_ds);
    pthread_mutex_unlock(&h->vm_lock);

    if (old_ds) randomx_release_dataset(old_ds);
    return RXSHARE_OK;
}

void rxshare_destroy(rxshare_handle *h)
{
    if (!h) return;

    pthread_mutex_lock(&h->build_lock);
    int in_progress = h->build_in_progress;
    pthread_mutex_unlock(&h->build_lock);
    if (in_progress) {
        pthread_join(h->build_thread, NULL);
        pthread_mutex_lock(&h->build_lock);
        if (h->pending_ds) {
            randomx_release_dataset(h->pending_ds);
            h->pending_ds = NULL;
        }
        h->build_in_progress = 0;
        pthread_mutex_unlock(&h->build_lock);
    }

    if (h->vm)            randomx_destroy_vm(h->vm);
    if (h->current_ds)    randomx_release_dataset(h->current_ds);
    if (h->current_cache) randomx_release_cache(h->current_cache);

    pthread_mutex_destroy(&h->vm_lock);
    pthread_mutex_destroy(&h->build_lock);
    free(h);
}
