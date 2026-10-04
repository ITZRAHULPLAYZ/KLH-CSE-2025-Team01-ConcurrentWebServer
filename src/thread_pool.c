/*
 * thread_pool.c
 * -------------
 * A simple fixed-size thread pool with a bounded connection queue.
 *
 * OS Concepts: POSIX threads (pthread_create), mutex (pthread_mutex_t),
 *              condition variables (pthread_cond_t), producer-consumer pattern.
 *
 * How it works:
 *   - Main thread calls queue_push(client_fd) after accept()
 *   - Worker threads block on queue_pop() waiting for a job
 *   - Mutex + condition variables keep everything safe and race-free
 */

#include "thread_pool.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

/* ── Forward declaration of the HTTP handler (defined in server.c) ─── */
extern void handle_client(int client_fd, int thread_id);

/* ── Internal state ──────────────────────────────────────────────────── */
static pthread_t  *workers = NULL;         /* thread handles            */
static int         actual_pool_size = 0;   /* dynamic pool size         */
static ConnQueue  *shared_queue = NULL;    /* pointer to shared queue   */
static int         shutdown_flag = 0;      /* set to 1 to stop workers  */

/* ═══════════════════════════════════════════════════════════════════════
 * QUEUE IMPLEMENTATION
 * ═══════════════════════════════════════════════════════════════════════ */

void queue_init(ConnQueue *q)
{
    q->head  = 0;
    q->tail  = 0;
    q->count = 0;

    pthread_mutex_init(&q->lock,      NULL);
    pthread_cond_init (&q->not_empty, NULL);
    pthread_cond_init (&q->not_full,  NULL);
}

/*
 * queue_push — called by the MAIN THREAD.
 * Blocks if queue is full (backpressure).
 */
void queue_push(ConnQueue *q, int client_fd)
{
    pthread_mutex_lock(&q->lock);

    /* wait while queue is full */
    while (q->count == QUEUE_SIZE)
        pthread_cond_wait(&q->not_full, &q->lock);

    q->fds[q->tail] = client_fd;
    q->tail = (q->tail + 1) % QUEUE_SIZE;   /* wrap around (circular) */
    q->count++;

    pthread_cond_signal(&q->not_empty);   /* wake up a sleeping worker */
    pthread_mutex_unlock(&q->lock);
}

/*
 * queue_pop — called by WORKER THREADS.
 * Blocks if queue is empty (workers sleep cheaply).
 * Returns -1 if the pool is shutting down.
 */
int queue_pop(ConnQueue *q)
{
    pthread_mutex_lock(&q->lock);

    /* wait while queue is empty AND not shutting down */
    while (q->count == 0 && !shutdown_flag)
        pthread_cond_wait(&q->not_empty, &q->lock);

    if (shutdown_flag && q->count == 0) {
        pthread_mutex_unlock(&q->lock);
        return -1;   /* signal to worker: time to exit */
    }

    int fd = q->fds[q->head];
    q->head = (q->head + 1) % QUEUE_SIZE;
    q->count--;

    pthread_cond_signal(&q->not_full);   /* main thread may be waiting */
    pthread_mutex_unlock(&q->lock);

    return fd;
}

void queue_destroy(ConnQueue *q)
{
    pthread_mutex_destroy(&q->lock);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
}

/* ═══════════════════════════════════════════════════════════════════════
 * WORKER THREAD FUNCTION
 * ═══════════════════════════════════════════════════════════════════════ */

static void *worker_func(void *arg)
{
    int thread_id = (int)(long)arg;   /* which worker number am I? */

    printf("[Thread-%d] started\n", thread_id);

    while (1) {
        int client_fd = queue_pop(shared_queue);

        if (client_fd == -1) {
            /* shutdown signal received */
            printf("[Thread-%d] exiting\n", thread_id);
            break;
        }

        /* Process the HTTP request for this client */
        handle_client(client_fd, thread_id);
        close(client_fd);
    }

    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════
 * POOL INIT / SHUTDOWN
 * ═══════════════════════════════════════════════════════════════════════ */

void thread_pool_init(ConnQueue *q, int pool_size)
{
    shared_queue = q;
    shutdown_flag = 0;
    actual_pool_size = pool_size;

    workers = (pthread_t *)malloc(sizeof(pthread_t) * pool_size);
    if (!workers) {
        perror("malloc workers");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < pool_size; i++) {
        if (pthread_create(&workers[i], NULL, worker_func, (void *)(long)i) != 0) {
            perror("pthread_create");
            exit(EXIT_FAILURE);
        }
    }
}

/*
 * thread_pool_shutdown — called when SIGINT is received.
 * Sets the shutdown flag, wakes all sleeping workers, then joins them.
 */
void thread_pool_shutdown(void)
{
    shutdown_flag = 1;

    /* wake ALL sleeping workers so they can see the shutdown flag */
    pthread_mutex_lock(&shared_queue->lock);
    pthread_cond_broadcast(&shared_queue->not_empty);
    pthread_mutex_unlock(&shared_queue->lock);

    /* wait for each worker to finish */
    for (int i = 0; i < actual_pool_size; i++) {
        pthread_join(workers[i], NULL);
        printf("[Thread-%d] joined\n", i);
    }
    
    free(workers);
}
