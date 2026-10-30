/*
 * thread_pool.h
 * Dynamic, elastic thread pool.
 *
 * Starts with MIN_THREADS workers. When a connection arrives and
 * all current workers are busy, a new detached thread is spawned
 * automatically (up to MAX_THREADS). Excess idle threads retire
 * themselves after IDLE_TIMEOUT seconds of inactivity.
 */

#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <pthread.h>
#include <time.h>

#define QUEUE_SIZE    1024   /* max pending connections in queue           */
#define MIN_THREADS   4      /* floor: always keep at least this many alive */
#define MAX_THREADS   1024   /* ceiling: hard cap on concurrent workers     */
#define IDLE_TIMEOUT  30     /* seconds idle before an excess thread exits  */

/* ── Bounded circular connection queue ─────────────────────────────── */
typedef struct {
    int  fds[QUEUE_SIZE];   /* circular buffer of client file-descriptors  */
    int  head;              /* next slot to read from                       */
    int  tail;              /* next slot to write to                        */
    int  count;             /* items currently in the queue                 */

    pthread_mutex_t lock;       /* guards all fields + pool counters        */
    pthread_cond_t  not_empty;  /* workers wait here when queue is empty    */
    pthread_cond_t  not_full;   /* main thread waits here when queue is full */
} ConnQueue;

/* ── Live pool health snapshot ──────────────────────────────────────── */
typedef struct {
    int active;       /* total threads alive                   */
    int idle;         /* threads blocked waiting for work      */
    int busy;         /* threads actively serving a client     */
    int queue_depth;  /* pending connections in the queue      */
} PoolStats;

/* Queue API */
void queue_init   (ConnQueue *q);
void queue_push   (ConnQueue *q, int client_fd);  /* main thread calls   */
int  queue_pop    (ConnQueue *q);                 /* workers call        */
void queue_destroy(ConnQueue *q);

/* Pool API */
void      thread_pool_init    (ConnQueue *q, int initial, int max_size);
void      thread_pool_shutdown(void);
PoolStats thread_pool_stats   (void);

#endif /* THREAD_POOL_H */
