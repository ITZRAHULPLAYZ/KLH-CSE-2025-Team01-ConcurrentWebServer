/*
 * thread_pool.c
 * -------------
 * Persistent POSIX worker thread pool with strict slot-based indexing.
 *
 * OS Concepts:
 *   - Persistent POSIX worker threads (pthread_create, detached)
 *   - Mutex + POSIX counting semaphores (sem_t) for producer-consumer bounded queue
 *   - Condition variables for worker notifications
 *   - sched_yield() for fair thread CPU scheduling
 *   - Per-thread performance tracking (status, lifetime request counter)
 *   - Configurable pool size: set at startup or resized on demand (CLI, SIGHUP, API)
 *
 * Invariant:
 *   The pool contains exactly N persistent worker threads (slots 0 to pool_size - 1).
 *   Thread IDs match slot numbers (T-0 to T-(N-1)).
 *   Threads do NOT auto-retire; they stay alive waiting for queued requests.
 */

#include "thread_pool.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <sched.h>
#include <sys/socket.h>

/* Forward declaration — defined in server.c */
extern void handle_client(int fd, int tid);

/* Forward declaration */
static void *worker_func(void *arg);

/* ── Pool state — guarded by shared_queue->lock ────────────────────── */
static ConnQueue *shared_queue      = NULL;
static int        active_threads    = 0;   /* total worker threads currently alive */
static int        idle_threads      = 0;   /* threads blocked waiting for work     */
static int        shutdown_flag     = 0;
static int        current_pool_size = DEFAULT_POOL_SIZE;

/* Per-thread slot tracker (indexed 0 to MAX_POOL_SIZE - 1) */
static ThreadInfo pool_threads[MAX_POOL_SIZE];

/* ── Shutdown completion tracking ──────────────────────────────────── */
static pthread_mutex_t done_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  done_cond  = PTHREAD_COND_INITIALIZER;
static int             done_count = 0;

/* ══════════════════════════════════════════════════════════════════════
 * QUEUE IMPLEMENTATION
 * ══════════════════════════════════════════════════════════════════════ */

void queue_init(ConnQueue *q)
{
    q->head = q->tail = q->count = 0;
    pthread_mutex_init(&q->lock,      NULL);
    sem_init(&q->items, 0, 0);           /* 0 ready connections initially */
    sem_init(&q->slots, 0, QUEUE_SIZE);  /* QUEUE_SIZE free slots initially */
    pthread_cond_init (&q->not_empty, NULL);
    pthread_cond_init (&q->not_full,  NULL);
}

/* Internal helper: caller MUST hold q->lock */
static int spawn_worker_locked(int slot)
{
    if (slot < 0 || slot >= MAX_POOL_SIZE) return -1;
    if (pool_threads[slot].active) return slot;

    pool_threads[slot].tid    = slot;
    pool_threads[slot].active = 1;
    pool_threads[slot].busy   = 0;

    pthread_t t;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    if (pthread_create(&t, &attr, worker_func, (void *)(long)slot) != 0) {
        perror("[Pool] pthread_create");
        pool_threads[slot].active = 0;
        pthread_attr_destroy(&attr);
        return -1;
    }
    pthread_attr_destroy(&attr);
    active_threads++;
    return slot;
}

/*
 * queue_push — called by the MAIN THREAD after accept().
 * Synchronizes queue capacity using POSIX counting semaphores
 * (sem_wait on slots, sem_post on items) and mutex for mutual exclusion.
 * Connections wait in the queue for the persistent worker threads.
 */
void queue_push(ConnQueue *q, int fd)
{
    /* POSIX Semaphore: wait for an available queue slot */
    sem_wait(&q->slots);

    pthread_mutex_lock(&q->lock);

    /* Block if queue is full */
    while (q->count == QUEUE_SIZE && !shutdown_flag)
        pthread_cond_wait(&q->not_full, &q->lock);

    if (shutdown_flag) {
        pthread_mutex_unlock(&q->lock);
        sem_post(&q->slots);
        close(fd);
        return;
    }

    q->fds[q->tail] = fd;
    q->tail = (q->tail + 1) % QUEUE_SIZE;
    q->count++;

    /* POSIX Semaphore: post that a new client connection item is available */
    sem_post(&q->items);

    /* Wake worker threads: broadcast under load so multiple idle workers activate */
    if (q->count > 1) {
        pthread_cond_broadcast(&q->not_empty);
    } else {
        pthread_cond_signal(&q->not_empty);
    }

    pthread_mutex_unlock(&q->lock);
}

/*
 * queue_pop — called by persistent WORKER THREAD for its assigned slot.
 * Workers sleep on cond_wait/semaphore until a request is pushed.
 */
int queue_pop(ConnQueue *q, int slot)
{
    pthread_mutex_lock(&q->lock);
    idle_threads++;

    while (q->count == 0 && !shutdown_flag) {
        /* If pool size was downsized, excess threads (slot >= current_pool_size) terminate cleanly */
        if (slot >= current_pool_size) {
            idle_threads--;
            active_threads--;
            pthread_mutex_unlock(&q->lock);
            printf("[Pool] Thread T-%d retired (pool downsized to %d)\n", slot, current_pool_size);
            return -2;
        }

        /* Persistent worker thread: wait for incoming requests */
        pthread_cond_wait(&q->not_empty, &q->lock);
    }

    idle_threads--;

    if (shutdown_flag && q->count == 0) {
        active_threads--;
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* If pool size was downsized while waiting */
    if (slot >= current_pool_size && q->count == 0) {
        active_threads--;
        pthread_mutex_unlock(&q->lock);
        return -2;
    }

    /* POSIX Semaphore: decrement available item count */
    sem_trywait(&q->items);

    int fd = q->fds[q->head];
    q->head = (q->head + 1) % QUEUE_SIZE;
    q->count--;

    /* POSIX Semaphore: post that a queue slot has freed up */
    sem_post(&q->slots);
    pthread_cond_signal(&q->not_full);

    pthread_mutex_unlock(&q->lock);
    return fd;
}

void queue_destroy(ConnQueue *q)
{
    pthread_mutex_destroy(&q->lock);
    sem_destroy(&q->items);
    sem_destroy(&q->slots);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
}

/* ══════════════════════════════════════════════════════════════════════
 * WORKER THREAD
 * ══════════════════════════════════════════════════════════════════════ */

static void *worker_func(void *arg)
{
    int slot = (int)(long)arg;
    int tid  = slot;
    printf("[Thread-%d] worker thread started in slot %d\n", tid, slot);

    while (1) {
        int fd = queue_pop(shared_queue, slot);

        if (fd < 0) {
            /* Thread exiting (shutdown or explicit pool downsize) */
            pthread_mutex_lock(&shared_queue->lock);
            pool_threads[slot].active = 0;
            pool_threads[slot].busy   = 0;
            pthread_mutex_unlock(&shared_queue->lock);

            const char *reason = (fd == -1) ? "shutdown" : "pool downsize";
            printf("[Thread-%d] exiting (%s)\n", tid, reason);

            pthread_mutex_lock(&done_mutex);
            done_count++;
            pthread_cond_signal(&done_cond);
            pthread_mutex_unlock(&done_mutex);
            return NULL;
        }

        /* Set read/write timeout on client socket so thread never hangs in recv */
        struct timeval tv = { .tv_sec = 2, .tv_usec = 0 };
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        /* Mark busy & increment request counter */
        pthread_mutex_lock(&shared_queue->lock);
        pool_threads[slot].busy = 1;
        pool_threads[slot].requests_count++;
        pthread_mutex_unlock(&shared_queue->lock);

        /* Handle client HTTP request */
        handle_client(fd, tid);
        close(fd);

        /* Mark idle immediately upon request completion */
        pthread_mutex_lock(&shared_queue->lock);
        pool_threads[slot].busy = 0;
        pthread_mutex_unlock(&shared_queue->lock);

        /* Yield CPU so other waiting worker threads get their turn */
        sched_yield();
    }
    return NULL;
}

/* ══════════════════════════════════════════════════════════════════════
 * POOL INIT / SHUTDOWN / STATS / RESIZE
 * ══════════════════════════════════════════════════════════════════════ */

void thread_pool_init(ConnQueue *q, int pool_size)
{
    shared_queue      = q;
    shutdown_flag     = 0;
    done_count        = 0;
    current_pool_size = (pool_size > 0 && pool_size <= MAX_POOL_SIZE) ? pool_size : DEFAULT_POOL_SIZE;

    for (int i = 0; i < MAX_POOL_SIZE; i++) {
        pool_threads[i].tid            = i;
        pool_threads[i].active         = 0;
        pool_threads[i].busy           = 0;
        pool_threads[i].requests_count = 0;
    }

    printf("[Pool] Initializing persistent worker pool with %d threads\n", current_pool_size);

    pthread_mutex_lock(&q->lock);
    for (int i = 0; i < current_pool_size; i++) {
        spawn_worker_locked(i);
    }
    pthread_mutex_unlock(&q->lock);
}

void thread_pool_shutdown(void)
{
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

PoolStats thread_pool_stats(void)
{
    PoolStats s;
    pthread_mutex_lock(&shared_queue->lock);
    s.pool_size   = current_pool_size;
    s.active      = active_threads;
    int b = 0;
    for (int i = 0; i < current_pool_size; i++) {
        if (pool_threads[i].active && pool_threads[i].busy) b++;
    }
    s.busy        = b;
    s.idle        = active_threads - b;
    s.queue_depth = shared_queue->count;
    pthread_mutex_unlock(&shared_queue->lock);
    return s;
}

int thread_pool_get_threads(ThreadInfo *out, int max_count)
{
    pthread_mutex_lock(&shared_queue->lock);
    int count = 0;
    for (int i = 0; i < current_pool_size && count < max_count; i++) {
        if (!pool_threads[i].active) continue;
        out[count] = pool_threads[i];
        count++;
    }
    pthread_mutex_unlock(&shared_queue->lock);
    return count;
}

int thread_pool_set_size(int new_size)
{
    if (new_size < 1) new_size = 1;
    if (new_size > MAX_POOL_SIZE) new_size = MAX_POOL_SIZE;

    pthread_mutex_lock(&shared_queue->lock);
    int old_size = current_pool_size;
    current_pool_size = new_size;

    if (new_size > old_size) {
        /* Increase pool size: spawn new persistent worker threads */
        for (int i = old_size; i < new_size; i++) {
            if (!pool_threads[i].active) {
                spawn_worker_locked(i);
            }
        }
    } else if (new_size < old_size) {
        /* Decrease pool size: signal waiting threads so slots >= new_size exit */
        pthread_cond_broadcast(&shared_queue->not_empty);
    }

    pthread_mutex_unlock(&shared_queue->lock);

    printf("[Pool] Pool size updated: %d -> %d persistent threads\n", old_size, new_size);
    logger_info("Thread pool size updated");
    return new_size;
}
