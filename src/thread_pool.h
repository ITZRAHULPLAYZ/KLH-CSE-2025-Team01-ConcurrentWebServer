#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <pthread.h>

#define QUEUE_SIZE  1024   /* max pending client connections */

/* ── Connection Queue ────────────────────────────────────────────────── */
typedef struct {
    int  fds[QUEUE_SIZE];   /* circular buffer of client file-descriptors */
    int  head;              /* next slot to read from                      */
    int  tail;              /* next slot to write to                       */
    int  count;             /* how many items are currently in the queue   */

    pthread_mutex_t lock;   /* protects all fields above                   */
    pthread_cond_t  not_empty; /* workers wait on this when queue is empty */
    pthread_cond_t  not_full;  /* main thread waits when queue is full     */
} ConnQueue;

/* ── Public API ──────────────────────────────────────────────────────── */
void queue_init(ConnQueue *q);
void queue_push(ConnQueue *q, int client_fd);   /* called by main thread  */
int  queue_pop (ConnQueue *q);                  /* called by workers       */
void queue_destroy(ConnQueue *q);

void thread_pool_init(ConnQueue *q, int pool_size);
void thread_pool_shutdown(void);

#endif /* THREAD_POOL_H */
