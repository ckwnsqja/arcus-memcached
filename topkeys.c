#include <sys/types.h>
#include <stdlib.h>
#include <assert.h>
#include <inttypes.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <memcached/genhash.h>
#include "topkeys.h"

static topkey_item_t *topkey_item_init(const void *key, int nkey, rel_time_t ctime) {
    topkey_item_t *item = calloc(sizeof(topkey_item_t) + nkey, 1);
    assert(item);
    assert(key);
    assert(nkey > 0);
    item->nkey = nkey;
    item->ctime = ctime;
    item->atime = ctime;
    // * Copy the key into the part trailing the struct * /
    memcpy(item->key, key, nkey);
    return item;
}

static inline size_t topkey_item_size(const topkey_item_t *item) {
    return sizeof(topkey_item_t) + item->nkey;
}

static inline topkey_item_t* topkeys_tail(topkeys_t *tk) {
    return (topkey_item_t*)(tk->list.prev);
}

static int my_hash_eq(const void *k1, size_t nkey1,
                      const void *k2, size_t nkey2) {
    return nkey1 == nkey2 && memcmp(k1, k2, nkey1) == 0;
}

#ifdef CSM
static csm_t *csm_init() {
    csm_t *csm = calloc(sizeof(csm_t),1);

    if (csm != NULL) {
        int i;
        for (i = 0; i < CSM_D; i++) {
            csm->hash[i] = 323 << (i + 1);
        }
    }

    return csm;
}
#endif

topkeys_t *topkeys_init(int max_keys) {
    topkeys_t *tk = calloc(sizeof(topkeys_t), 1);
    if (tk == NULL) {
        return NULL;
    }

    pthread_mutex_init(&tk->mutex, NULL);
    tk->max_keys = max_keys;
    tk->list.next = &tk->list;
    tk->list.prev = &tk->list;
    static struct hash_ops my_hash_ops = {
        .hashfunc = genhash_string_hash,
        .hasheq = my_hash_eq,
        .dupKey = NULL,
        .dupValue = NULL,
        .freeKey = NULL,
        .freeValue = NULL,
    };
#ifdef CSM
    tk->csm = csm_init();
    if (tk->csm == NULL) {
        return NULL;
    }
#endif
    tk->hash = genhash_init(max_keys, my_hash_ops);
    if (tk->hash == NULL) {
        return NULL;
    }
    printf("max_keys : %d\n",max_keys);
    return tk;
}

void topkeys_free(topkeys_t *tk) {
    pthread_mutex_destroy(&tk->mutex);
    genhash_free(tk->hash);
    dlist_t *p = tk->list.next;
    while (p != &tk->list) {
        dlist_t *tmp = p->next;
        free(p);
        p = tmp;
    }
}

static inline void dlist_remove(dlist_t *list) {
    assert(list->prev->next == list);
    assert(list->next->prev == list);
    list->prev->next = list->next;
    list->next->prev = list->prev;
}

static inline void dlist_insert_after(dlist_t *list, dlist_t *new) {
    new->next = list->next;
    new->prev = list;
    list->next->prev = new;
    list->next = new;
}

static inline void dlist_iter(dlist_t *list,
                              void (*iterfunc)(dlist_t *item, void *arg),
                              void *arg)
{
    dlist_t *p = list;
    while ((p = p->next) != list) {
        iterfunc(p, arg);
    }
}

static inline void topkeys_item_delete(topkeys_t *tk, topkey_item_t *item) {
    genhash_delete(tk->hash, item->key, item->nkey);
    dlist_remove(&item->list);
#ifndef LC
    --tk->nkeys;
#endif
    free(item);
}

#ifdef HOT_ITEMS
static void print_out_list(topkeys_t *tk) {
    dlist_t *p = &tk->list;
    printf("HOT ITEMS LIST\n");

    while ((p = p->next) != &tk->list) {
        printf("%s : %d\n", ((topkey_item_t *)p)->key, ((topkey_item_t *)p)->counter);
    }
    printf("END\n");
}


#ifdef LC
static void *topkeys_lossy(topkeys_t *tk, const void *key, size_t nkey, const rel_time_t ctime) {
    topkey_item_t *item = genhash_find(tk->hash, key, nkey);
    ++tk->N;
    if (item == NULL) {
        item = topkey_item_init(key, nkey, ctime);
        item->counter = tk->delta + 1;
        if (item != NULL) {
            genhash_update(tk->hash, item->key, item->nkey, item, topkey_item_size(item));
        } else {
            return NULL;
        }
    } else {
        ++item->counter;
        dlist_remove(&item->list);
        //count++ & dlist_remove
    }

    dlist_t *p = &tk->list;
    while ((p = p->next) != &tk->list) {
        if (((topkey_item_t *)p)->counter >= item->counter) {
            break;
        }
    }
    dlist_insert_after(p->prev, &item->list);

    if (tk->delta != (++tk->nkeys) / (tk->max_keys)) {
        tk->delta = (tk->nkeys) / (tk->max_keys);
        p = &tk->list;
        while ((p = tk->list.next) != &tk->list) {
            if (((topkey_item_t*)p)->counter < tk->delta) {
                topkeys_item_delete(tk, (topkey_item_t *)p);
            } else {
                break;
            }
        }
    }

    if (tk->N == 100000){
        print_out_list(tk);
        printf("Total operating time is %f.\n", tk->op_time);
    }
    return item;
}
#endif

#ifdef SSL
static void *topkeys_space_saving(topkeys_t *tk, const void *key, size_t nkey, const rel_time_t ctime) {
    topkey_item_t *item = genhash_find(tk->hash, key, nkey);
    ++tk->N;
    if (item == NULL) {
        item = topkey_item_init(key, nkey, ctime);
        if (item != NULL) {
            genhash_update(tk->hash, item->key, item->nkey, item, topkey_item_size(item));
            
            if(++tk->nkeys <= tk->max_keys) {
                item->counter = 1;
            } else {
                dlist_t *p = tk->list.next;
                item->error_value = ((topkey_item_t *)p)->counter;
                item->counter = item->error_value + 1;
                topkeys_item_delete(tk, (topkey_item_t *)p);
            }
        } else {
            return NULL;
        }
    } else {
        ++item->counter;
        dlist_remove(&item->list);
    }

    dlist_t *p = &tk->list;
    while ((p = p->next) != &tk->list) {
        if (((topkey_item_t *)p)->counter >= item->counter)
            break;
    }
    dlist_insert_after(p->prev, &item->list);

    if (tk->N == 100000) {
        print_out_list(tk);
        printf("Total operating time is %f.\n",tk->op_time);
    }
    return item;
}
#endif

#ifdef CSM
static int genhash_string_hash_csm(const void* p, size_t nkey, int rv)
{
    int i = 0;
    char *str = (char *)p;

    for (i = 0; i < nkey; i++) {
       rv = ((rv << 1) + rv) ^ str[i];
    }

    return rv;
}

static int add_count(topkeys_t *tk, const void *key, size_t nkey) {
    int i;
    int mask = CSM_W - 1;
 
    for (i = 0; i < CSM_D; i++) {
        int val =  genhash_string_hash_csm(key, nkey, tk->csm->hash[i]);
        val &= mask;
        tk->csm->cm[i][val] += 1;        
    }    
    return 0;
}

static int estimate_count(topkeys_t *tk, const void *key, size_t nkey) {
    int min = 0x7fffffff;
    int i;
    int mask = CSM_W - 1;

    for (i = 0; i < CSM_D; i++) {
        int val = genhash_string_hash_csm(key, nkey, tk->csm->hash[i]);
        val &= mask;
        if (min > tk->csm->cm[i][val]) {
            min = tk->csm->cm[i][val];
        }
    }     
    return min;
}

static void *topkeys_count_sketch_min(topkeys_t *tk, const void *key, size_t nkey, const rel_time_t ctime) {
    topkey_item_t *item = genhash_find(tk->hash, key, nkey);
    add_count(tk, key, nkey);
    ++tk->N;

    if (tk->N == 100000){
        print_out_list(tk);
        printf("Total operating time is %f.\n", tk->op_time);
    }

    if (item == NULL) {
        item = topkey_item_init(key, nkey, ctime);
        if (item != NULL) {
            if (++tk->nkeys < tk->max_keys) {
                genhash_update(tk->hash, item->key, item->nkey, item, topkey_item_size(item));
                item->counter = estimate_count(tk, key, nkey);
            } else {
                dlist_t *del = tk->list.next;

                if (((topkey_item_t *)del)->counter < item->counter) {
                    genhash_update(tk->hash, item->key, item->nkey, item, topkey_item_size(item));
                    item->counter = estimate_count(tk, key, nkey);
                    topkeys_item_delete(tk, (topkey_item_t *)del);
                } else {
                    free(item);
                    --tk->nkeys;
                    return del;
                }
            }
        } else {
            return NULL;
        }
    } else {
        ++item->counter;
        dlist_remove(&item->list);
    }

    dlist_t *p = &tk->list;
    while ((p = p->next) != &tk->list) {
        if (((topkey_item_t *)p)->counter >= item->counter) {
            break;
        }
    }
    dlist_insert_after(p->prev, &item->list);
    return item;
}
#endif
#endif

topkey_item_t *topkeys_item_get_or_create(topkeys_t *tk, const void *key, size_t nkey, const rel_time_t ctime) {
#ifdef HOT_ITEMS
    topkey_item_t *ret;
    struct timeval start_point, end_point;
    double operating_time;
    gettimeofday(&start_point, NULL);

#ifdef LC
    ret = (topkey_item_t *)topkeys_lossy(tk, key, nkey, ctime);
#endif
#ifdef SSL
    ret = (topkey_item_t *)topkeys_space_saving(tk, key, nkey, ctime);
#endif
#ifdef CSM
    ret = (topkey_item_t *)topkeys_count_sketch_min(tk, key, nkey, ctime);
#endif

    gettimeofday(&end_point, NULL);
    operating_time = (double)(end_point.tv_sec) + (double)(end_point.tv_usec) / 1000000.0 - (double)(start_point.tv_sec) - (double)(start_point.tv_usec) / 1000000.0;
    tk->op_time += operating_time;
    return ret;
#else
    topkey_item_t *item = genhash_find(tk->hash, key, nkey);
    if (item == NULL) {
        item = topkey_item_init(key, nkey, ctime);
        if (item != NULL) {
            if (++tk->nkeys > tk->max_keys) {
                topkeys_item_delete(tk, topkeys_tail(tk));
            }
            genhash_update(tk->hash, item->key, item->nkey,
                           item, topkey_item_size(item));
        } else {
            return NULL;
        }
    } else {
        dlist_remove(&item->list);
    }
    dlist_insert_after(&tk->list, &item->list);
    return item;
#endif
}


/**** unused function ****
static inline void append_stat(const void *cookie,
                               const char *name,
                               size_t namelen,
                               const char *key,
                               size_t nkey,
                               int value,
                               ADD_STAT add_stats) {
    char key_str[128];
    char val_str[128];
    int klen, vlen;

    klen = sizeof(key_str) - namelen - 2;
    if (nkey < klen) {
        klen = nkey;
    }
    memcpy(key_str, key, klen);
    key_str[klen] = '.';
    memcpy(&key_str[klen+1], name, namelen + 1);
    klen += namelen + 1;
    vlen = snprintf(val_str, sizeof(val_str) - 1, "%d", value);
    add_stats(key_str, klen, val_str, vlen, cookie);
}
****************************/

struct tk_context {
    const void *cookie;
    ADD_STAT add_stat;
    rel_time_t current_time;
};

#define TK_FMT(name) #name "=%d,"
#define TK_ARGS(name) item->name,

static void tk_iterfunc(dlist_t *list, void *arg) {
    struct tk_context *c = arg;
    topkey_item_t *item = (topkey_item_t*)list;
    char val_str[TK_MAX_VAL_LEN];
    /* This line is magical. The missing comma before item->ctime is because the TK_ARGS macro ends with a comma. */
    int vlen = snprintf(val_str, sizeof(val_str) - 1, TK_OPS(TK_FMT)"ctime=%"PRIu32",atime=%"PRIu32, TK_OPS(TK_ARGS)
                        c->current_time - item->ctime, c->current_time - item->atime);
    c->add_stat(item->key, item->nkey, val_str, vlen, c->cookie);
}

ENGINE_ERROR_CODE topkeys_stats(topkeys_t *tk,
                                const void *cookie,
                                const rel_time_t current_time,
                                ADD_STAT add_stat) {
    struct tk_context context;
    context.cookie = cookie;
    context.add_stat = add_stat;
    context.current_time = current_time;
    assert(tk);
    pthread_mutex_lock(&tk->mutex);
    dlist_iter(&tk->list, tk_iterfunc, &context);
    pthread_mutex_unlock(&tk->mutex);
    return ENGINE_SUCCESS;
}
