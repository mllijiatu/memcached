/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <signal.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t maintenance_lock = PTHREAD_MUTEX_INITIALIZER;

/* how many powers of 2's worth of buckets we use */
unsigned int hashpower = HASHPOWER_DEFAULT;

#define hashsize(n) ((uint64_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/* Main hash table. This is where we look except during expansion. */
static item** primary_hashtable = 0;

/*
 * Previous hash table. During expansion, we look here for keys that haven't
 * been moved over to the primary yet.
 */
static item** old_hashtable = 0;

/* Flag: Are we in the middle of expanding now? */
static bool expanding = false;

/*
 * During expansion we migrate values with bucket granularity; this is how
 * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
 */
static uint64_t expand_bucket = 0;

// 初始化 HashTable 表
void assoc_init(const int hashtable_init) {
    //初始化的时候 hashtable_init值需要大于12 小于64
    //如果hashtable_init的值没有设定，则 hash power 使用默认值为16
    if (hashtable_init) {
        hashpower = hashtable_init;
    }
    // primary_hashtable主要用来存储这个HashTable
    // hash size方法是求桶的个数，默认如果hash power=16的话，桶的个数为：65536
    primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
    if (! primary_hashtable) {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }
    STATS_LOCK();
    stats.hash_power_level = hashpower;
    stats.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

// 寻找一个Item
item *assoc_find(const char *key, const size_t nkey, const uint32_t hv) {
    item *it;
    unsigned int oldbucket;

    // 判断是否在扩容中...
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it = old_hashtable[oldbucket];
    } else {
        // 获取得到具体的桶的地址
        it = primary_hashtable[hv & hashmask(hashpower)];
    }

    item *ret = NULL;
    int depth = 0; // 循环的深度
    while (it) {
        //循环查找桶的list中的Item
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/**
 * 返回指向键之前的项目指针的地址。如果 *item == 0，表示未找到项目。
 *
 * @param key 键的指针
 * @param nkey 键的长度
 * @param hv 哈希值
 * @return 指向项目指针的地址
 */
static item** _hashitem_before(const char *key, const size_t nkey, const uint32_t hv) {
    item **pos;          // 指向项目指针的指针
    uint64_t oldbucket;   // 旧的桶

    // 如果正在扩展并且哈希值对应的旧桶大于等于扩展的桶
    if (expanding && (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket) {
        pos = &old_hashtable[oldbucket];
    } else {
        pos = &primary_hashtable[hv & hashmask(hashpower)];
    }

    // 在哈希链表上查找键值对应的项目
    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }

    return pos;
}


/**
 * 将哈希表扩展到下一个2的幂。
 */
static void assoc_expand(void) {
    old_hashtable = primary_hashtable;  // 保存旧的哈希表

    // 为新的主哈希表分配内存
    primary_hashtable = calloc(hashsize(hashpower + 1), sizeof(void *));

    // 如果成功分配了内存
    if (primary_hashtable) {
        // 输出扩展哈希表的信息
        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");

        // 更新哈希表的大小和状态
        hashpower++;
        expanding = true;
        expand_bucket = 0;

        STATS_LOCK();
        stats_state.hash_power_level = hashpower;
        stats_state.hash_bytes += hashsize(hashpower) * sizeof(void *);
        stats_state.hash_is_expanding = true;
        STATS_UNLOCK();
    } else {
        // 如果分配内存失败，回滚操作，继续运行
        primary_hashtable = old_hashtable;
        /* Bad news, but we can keep running. */
    }
}


/**
 * 开始扩展哈希表，如果当前项目数超过哈希表容量的3/2并且哈希表大小尚未达到最大值。
 *
 * @param curr_items 当前项目数
 */
void assoc_start_expand(uint64_t curr_items) {
    // 尝试获取维护锁
    if (pthread_mutex_trylock(&maintenance_lock) == 0) {
        // 如果当前项目数超过哈希表容量的3/2并且哈希表大小尚未达到最大值
        if (curr_items > (hashsize(hashpower) * 3) / 2 && hashpower < HASHPOWER_MAX) {
            // 发送维护条件信号，以便开始扩展哈希表
            pthread_cond_signal(&maintenance_cond);
        }

        // 释放维护锁
        pthread_mutex_unlock(&maintenance_lock);
    }
}


// 新增Item操作
int assoc_insert(item *it, const uint32_t hv) {
    uint64_t oldbucket;

//    assert(assoc_find(ITEM_key(it), it->nkey) == 0);  /* shouldn't have duplicately named things defined */

    //判断是否在扩容，如果是扩容中，为保证程序继续可用，则需要使用旧的桶
    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it->h_next = old_hashtable[oldbucket];
        old_hashtable[oldbucket] = it;
    } else {
        // hv & hashmask(hashpower) 按位与计算是在哪个桶上面
        // 将当前的item->h_next 指向桶中首个Item的位置
        it->h_next = primary_hashtable[hv & hashmask(hashpower)];
        // 然后将hashtable中的首页Item指向新的Item地址值
        primary_hashtable[hv & hashmask(hashpower)] = it;
    }

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey);
    return 1;
}

/**
 * 从关联数组中删除给定键的项目。
 *
 * @param key 要删除的项目的键的指针
 * @param nkey 键的长度
 * @param hv 哈希值
 */
void assoc_delete(const char *key, const size_t nkey, const uint32_t hv) {
    item **before = _hashitem_before(key, nkey, hv);

    // 如果找到要删除的项目
    if (*before) {
        item *nxt;

        // 触发DTrace探针，用于性能分析
        MEMCACHED_ASSOC_DELETE(key, nkey);

        // 保存下一个项目的指针
        nxt = (*before)->h_next;

        // 将当前项目从哈希链表中移除
        (*before)->h_next = 0;   /* 可能没有意义，但随便写点什么。 */
        *before = nxt;
        return;
    }

    // 注意：实际上不会执行到这里。调用者不会删除找不到的项目。
    assert(*before != 0);
}



static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

/**
 * 关联数组维护线程的主函数。
 *
 * @param arg 未使用的参数
 * @return 无
 */
static void *assoc_maintenance_thread(void *arg) {
    // 获取维护锁
    mutex_lock(&maintenance_lock);

    // 在维护线程需要运行的情况下执行循环
    while (do_run_maintenance_thread) {
        int ii = 0;

        /* 只有一个扩展线程，所以不需要全局锁。 */
        for (ii = 0; ii < hash_bulk_move && expanding; ++ii) {
            item *it, *next;
            uint64_t bucket;
            void *item_lock = NULL;

            /* bucket = hv & hashmask(hashpower) =>哈希表的桶
             * 是hv的最低N位，item_locks的桶也是hv的最低M位，
             * 而N大于M。因此我们可以只用一个item_lock处理扩展。*/
            if ((item_lock = item_trylock(expand_bucket))) {
                for (it = old_hashtable[expand_bucket]; NULL != it; it = next) {
                    next = it->h_next;
                    bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashpower);
                    it->h_next = primary_hashtable[bucket];
                    primary_hashtable[bucket] = it;
                }

                old_hashtable[expand_bucket] = NULL;

                expand_bucket++;
                if (expand_bucket == hashsize(hashpower - 1)) {
                    // 扩展完成，进行清理工作
                    expanding = false;
                    free(old_hashtable);

                    STATS_LOCK();
                    stats_state.hash_bytes -= hashsize(hashpower - 1) * sizeof(void *);
                    stats_state.hash_is_expanding = false;
                    STATS_UNLOCK();

                    if (settings.verbose > 1)
                        fprintf(stderr, "Hash table expansion done\n");
                }

            } else {
                usleep(10 * 1000);
            }

            if (item_lock) {
                item_trylock_unlock(item_lock);
                item_lock = NULL;
            }
        }

        if (!expanding) {
            /* 扩展完成后等待下一次调用 */
            pthread_cond_wait(&maintenance_cond, &maintenance_lock);

            /* assoc_expand()完全替换了哈希表，因此我们需要
             * 在此期间，所有线程不应持有与哈希表相关的任何引用。
             * 这是一个相对较简单、可能更慢的算法，
             * 允许在不引起显著等待时间的情况下进行动态哈希表扩展。*/
            if (do_run_maintenance_thread) {
                pause_threads(PAUSE_ALL_THREADS);
                assoc_expand();
                pause_threads(RESUME_ALL_THREADS);
            }
        }
    }

    // 释放维护锁
    mutex_unlock(&maintenance_lock);
    return NULL;
}


static pthread_t maintenance_tid;

/**
 * 启动关联数组维护线程。
 *
 * @return 成功返回 0，失败返回 -1
 */
int start_assoc_maintenance_thread(void) {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");

    // 设置哈希批量移动的参数
    if (env != NULL) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }

    // 创建维护线程
    if ((ret = pthread_create(&maintenance_tid, NULL, assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }

    // 设置维护线程的名称
    thread_setname(maintenance_tid, "mc-assocmaint");
    return 0;
}

/**
 * 停止关联数组维护线程。
 */
void stop_assoc_maintenance_thread(void) {
    // 获取维护锁
    mutex_lock(&maintenance_lock);

    // 设置维护线程退出标志，并发送条件信号
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);

    // 释放维护锁
    mutex_unlock(&maintenance_lock);

    /* 等待维护线程停止 */
    pthread_join(maintenance_tid, NULL);
}


struct assoc_iterator {
    uint64_t bucket;
    item *it;
    item *next;
    bool bucket_locked;
};

void *assoc_get_iterator(void) {
    struct assoc_iterator *iter = calloc(1, sizeof(struct assoc_iterator));
    if (iter == NULL) {
        return NULL;
    }
    // this will hang the caller while a hash table expansion is running.
    mutex_lock(&maintenance_lock);
    return iter;
}

bool assoc_iterate(void *iterp, item **it) {
    struct assoc_iterator *iter = (struct assoc_iterator *) iterp;
    *it = NULL;
    // - if locked bucket and next, update next and return
    if (iter->bucket_locked) {
        if (iter->next != NULL) {
            iter->it = iter->next;
            iter->next = iter->it->h_next;
            *it = iter->it;
        } else {
            // unlock previous bucket, if any
            item_unlock(iter->bucket);
            // iterate the bucket post since it starts at 0.
            iter->bucket++;
            iter->bucket_locked = false;
            *it = NULL;
        }
        return true;
    }

    // - loop until we hit the end or find something.
    if (iter->bucket != hashsize(hashpower)) {
        // - lock next bucket
        item_lock(iter->bucket);
        iter->bucket_locked = true;
        // - only check the primary hash table since expand is blocked.
        iter->it = primary_hashtable[iter->bucket];
        if (iter->it != NULL) {
            // - set it, next and return
            iter->next = iter->it->h_next;
            *it = iter->it;
        } else {
            // - nothing found in this bucket, try next.
            item_unlock(iter->bucket);
            iter->bucket_locked = false;
            iter->bucket++;
        }
    } else {
        return false;
    }

    return true;
}

void assoc_iterate_final(void *iterp) {
    struct assoc_iterator *iter = (struct assoc_iterator *) iterp;
    if (iter->bucket_locked) {
        item_unlock(iter->bucket);
    }
    mutex_unlock(&maintenance_lock);
    free(iter);
}
