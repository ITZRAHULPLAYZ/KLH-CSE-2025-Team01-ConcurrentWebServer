/*
 * thread_pool.h
 * -------------
 * Persistent POSIX worker thread pool for concurrent request processing.
 *
 * Spawns a dedicated pool of N persistent worker threads at startup.
 * Worker threads sleep efficiently on condition variables and semaphores
 * waiting for incoming client connections in the shared ConnQueue.
 * Pool size is configurable and can be increased or adjusted at runtime
 * (via CLI, SIGHUP config reload, or API) without restarting the server.
 */

#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <pthread.h>
#include <semaphore.h>
#include <time.h>

#define QUEUE_SIZE         1024   /* max pending connections in queue      */
#define DEFAULT_POOL_SIZE  16     /* standard worker threads in pool       */
#define MAX_POOL_SIZE      1024   /* hard cap on maximum pool size         */

/* ── Bounded circular connection queue with Mutex & Semaphores ──────── */
typedef struct {
    int  fds[QUEUE_SIZE];   /* circular buffer of client file-descriptors  */
    int  head;              /* next slot to read from                       */
    int  tail;              /* next slot to write to                        */
    int  count;             /* items currently in the queue                 */

    pthread_mutex_t lock;       /* mutual exclusion lock for critical section   */
    sem_t           items;      /* counting semaphore: available ready requests */
    sem_t           slots;      /* counting semaphore: available queue slots    */
    pthread_cond_t  not_empty;  /* workers wait here when queue is empty        */
    pthread_cond_t  not_full;   /* main thread waits here when queue is full    */
} ConnQueue;

/* ── Live pool health snapshot ──────────────────────────────────────── */
typedef struct {
    int pool_size;    /* configured persistent pool size       */
    int active;       /* total worker threads alive in pool    */
    int idle;         /* threads blocked waiting for work      */
    int busy;         /* threads actively serving a client     */
    int queue_depth;  /* pending connections in the queue      */
} PoolStats;

/* Queue API */
void queue_init   (ConnQueue *q);
void queue_push   (ConnQueue *q, int client_fd);  /* main thread calls   */
int  queue_pop    (ConnQueue *q, int slot);       /* workers call        */
void queue_destroy(ConnQueue *q);

/* ── Per-thread status and request metrics ──────────────────────────── */
typedef struct {
    int  tid;             /* thread ID (0, 1, 2, ...)              */
    int  active;          /* 1 if alive, 0 if retired              */
    int  busy;            /* 1 if actively serving, 0 if idle      */
    long requests_count;  /* total requests served by this thread  */
} ThreadInfo;

/* Pool API */
void      thread_pool_init       (ConnQueue *q, int pool_size);
void      thread_pool_shutdown   (void);
PoolStats thread_pool_stats      (void);
int       thread_pool_set_size   (int new_size);
int       thread_pool_get_threads(ThreadInfo *out, int max_count);

#endif /* THREAD_POOL_H */
