/*
 * thread_pool.c
 * -------------
 * Elastic dynamic thread pool with on-demand scaling.
 *
 * OS Concepts:
 *   POSIX threads (pthread_create, detached),
 *   Mutex + condition variables (producer-consumer),
 *   pthread_cond_timedwait for idle thread retirement.
 *
 * Scaling rules:
 *   - Spawns a new detached worker whenever a connection arrives and
 *     idle_threads == 0 AND active_threads < max_pool.
 *   - Excess idle threads (above min_pool) retire themselves after
 *     IDLE_TIMEOUT seconds of inactivity.
 *   - Core threads (up to min_pool) block indefinitely — never retire.
 */

#include "thread_pool.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>

/* Forward declaration — defined in server.c */
extern void handle_client(int fd, int tid);

/* Forward declaration — defined below */
static void *worker_func(void *arg);

/* ── Pool state — all guarded by shared_queue->lock ────────────────── */
static ConnQueue *shared_queue   = NULL;
static int        active_threads = 0;   /* total threads currently alive  */
static int        idle_threads   = 0;   /* threads blocked in condwait    */
static int        shutdown_flag  = 0;
static int        min_pool       = MIN_THREADS;
static int        max_pool       = MAX_THREADS;
static int        tid_seq        = 0;   /* monotonic thread-id generator  */

/* ── Shutdown completion tracking ──────────────────────────────────── */
static pthread_mutex_t done_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  done_cond  = PTHREAD_COND_INITIALIZER;
static int             done_count = 0;   /* threads that have exited       */

/* ══════════════════════════════════════════════════════════════════════
 * QUEUE IMPLEMENTATION
 * ══════════════════════════════════════════════════════════════════════ */

void queue_init(ConnQueue *q)
{
    q->head = q->tail = q->count = 0;
    pthread_mutex_init(&q->lock,      NULL);
    pthread_cond_init (&q->not_empty, NULL);
    pthread_cond_init (&q->not_full,  NULL);
}

/*
 * queue_push — called by the MAIN THREAD after accept().
 *
 * If all current workers are busy (idle_threads == 0) and we haven't
 * hit the ceiling, we spawn a new detached worker on the spot.
 * This is the dynamic scaling trigger.
 */
void queue_push(ConnQueue *q, int fd)
{
    pthread_mutex_lock(&q->lock);

    /* Block if queue is full (backpressure) */
    while (q->count == QUEUE_SIZE && !shutdown_flag)
        pthread_cond_wait(&q->not_full, &q->lock);

    if (shutdown_flag) {
        pthread_mutex_unlock(&q->lock);
        close(fd);
        return;
    }

    q->fds[q->tail] = fd;
    q->tail = (q->tail + 1) % QUEUE_SIZE;
    q->count++;

    /* ── Dynamic scale-up ──────────────────────────────────────────
     * Spawn a new worker if no thread is free and we're below cap.
     * active_threads is incremented HERE (under lock) before the
     * pthread_create call to prevent double-spawn races.
     * ────────────────────────────────────────────────────────────── */
    if (idle_threads == 0 && active_threads < max_pool) {
        active_threads++;
        int new_tid = tid_seq++;

        pthread_t t;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

        if (pthread_create(&t, &attr, worker_func, (void *)(long)new_tid) != 0) {
            perror("[Pool] pthread_create");
            active_threads--;   /* undo pre-increment on failure */
        } else {
            printf("[Pool] +Thread-%d spawned  [active=%d idle=%d]\n",
                   new_tid, active_threads, idle_threads);
            logger_info("Thread pool scaled up");
        }
        pthread_attr_destroy(&attr);
    }

    pthread_cond_signal(&q->not_empty);   /* wake one waiting worker */
    pthread_mutex_unlock(&q->lock);
}

/*
 * queue_pop — called by WORKER THREADS.
 *
 * Core threads (first min_pool) wait forever.
 * Excess threads time-out after IDLE_TIMEOUT and return -2 to retire.
 * Returns -1 on shutdown, -2 on idle timeout, >=0 for a valid fd.
 */
int queue_pop(ConnQueue *q)
{
    pthread_mutex_lock(&q->lock);
    idle_threads++;

    while (q->count == 0 && !shutdown_flag) {
        if (active_threads > min_pool) {
            /* Excess thread: use timed wait */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += IDLE_TIMEOUT;

            int r = pthread_cond_timedwait(&q->not_empty, &q->lock, &ts);
            if (r == ETIMEDOUT && q->count == 0 && active_threads > min_pool) {
                /* Idle too long — retire this excess thread */
                idle_threads--;
                active_threads--;
                int remaining = active_threads;
                pthread_mutex_unlock(&q->lock);
                printf("[Pool] -Thread idle timeout  [active=%d]\n", remaining);
                return -2;
            }
        } else {
            /* Core thread: wait indefinitely */
            pthread_cond_wait(&q->not_empty, &q->lock);
        }
    }

    idle_threads--;

    /* Shutdown with empty queue — time to exit */
    if (shutdown_flag && q->count == 0) {
        active_threads--;
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Dequeue one connection (works during shutdown too — drains queue) */
    int fd = q->fds[q->head];
    q->head = (q->head + 1) % QUEUE_SIZE;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);
    return fd;
}

void queue_destroy(ConnQueue *q)
{
    pthread_mutex_destroy(&q->lock);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
}

/* ══════════════════════════════════════════════════════════════════════
 * WORKER THREAD
 * ══════════════════════════════════════════════════════════════════════ */

static void *worker_func(void *arg)
{
    int tid = (int)(long)arg;
    printf("[Thread-%d] started\n", tid);

    while (1) {
        int fd = queue_pop(shared_queue);

        if (fd < 0) {
            /* -1 = shutdown, -2 = idle timeout retirement */
            const char *reason = (fd == -1) ? "shutdown" : "idle timeout";
            printf("[Thread-%d] exiting (%s)\n", tid, reason);

            /* Notify thread_pool_shutdown() that one more thread is done */
            pthread_mutex_lock(&done_mutex);
            done_count++;
            pthread_cond_signal(&done_cond);
            pthread_mutex_unlock(&done_mutex);
            return NULL;
        }

        handle_client(fd, tid);
        close(fd);
    }
    return NULL;
}

/* ══════════════════════════════════════════════════════════════════════
 * POOL INIT / SHUTDOWN / STATS
 * ══════════════════════════════════════════════════════════════════════ */

void thread_pool_init(ConnQueue *q, int initial, int max_size)
{
    shared_queue  = q;
    shutdown_flag = 0;
    done_count    = 0;
    min_pool      = (initial  > 0)           ? initial   : MIN_THREADS;
    max_pool      = (max_size > min_pool)    ? max_size  : MAX_THREADS;

    printf("[Pool] Starting %d core threads (max=%d, idle_timeout=%ds)\n",
           min_pool, max_pool, IDLE_TIMEOUT);

    pthread_mutex_lock(&q->lock);
    for (int i = 0; i < min_pool; i++) {
        active_threads++;
        int new_tid = tid_seq++;

        pthread_t t;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

        if (pthread_create(&t, &attr, worker_func, (void *)(long)new_tid) != 0) {
            perror("pthread_create");
            active_threads--;
        }
        pthread_attr_destroy(&attr);
    }
    pthread_mutex_unlock(&q->lock);
}

/*
 * thread_pool_shutdown — called when SIGINT/SIGTERM is received.
 * Snapshot the current thread count, raise the flag, wake everyone,
 * then wait (up to 10 s) for all threads to signal done_cond.
 */
void thread_pool_shutdown(void)
{
    /* Capture target count and set flag atomically under the queue lock */
    pthread_mutex_lock(&shared_queue->lock);
    int target = active_threads;
    shutdown_flag = 1;
    pthread_cond_broadcast(&shared_queue->not_empty);
    pthread_mutex_unlock(&shared_queue->lock);

    printf("[Pool] Shutdown: waiting for %d thread(s) to exit...\n", target);

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 10;

    pthread_mutex_lock(&done_mutex);
    while (done_count < target) {
        int r = pthread_cond_timedwait(&done_cond, &done_mutex, &deadline);
        if (r == ETIMEDOUT) {
            printf("[Pool] Warning: shutdown timeout (%d/%d exited)\n",
                   done_count, target);
            break;
        }
    }
    pthread_mutex_unlock(&done_mutex);

    printf("[Pool] All threads done.\n");
}

/*
 * thread_pool_stats — safe snapshot of current pool health.
 * Called by the /api/stats handler in server.c.
 */
PoolStats thread_pool_stats(void)
{
    PoolStats s;
    pthread_mutex_lock(&shared_queue->lock);
    s.active      = active_threads;
    s.idle        = idle_threads;
    s.busy        = active_threads - idle_threads;
    s.queue_depth = shared_queue->count;
    pthread_mutex_unlock(&shared_queue->lock);
    return s;
}
