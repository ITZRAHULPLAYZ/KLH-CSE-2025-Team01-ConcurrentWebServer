/*
 * logger.c
 * --------
 * Thread-safe request logger.
 *
 * OS Concepts: mutex for mutual exclusion, file I/O.
 *
 * Usage:
 *   logger_init("server.log");
 *   logger_log(tid, "GET", "/index.html", 200);
 *   logger_close();
 */

#include "logger.h"

#include <stdio.h>
#include <time.h>
#include <pthread.h>

/* ── Internal state ─────────────────────────────────────────────────── */
static FILE            *log_fp   = NULL;
static pthread_mutex_t  log_lock = PTHREAD_MUTEX_INITIALIZER;

/* ── Helpers ─────────────────────────────────────────────────────────── */
static void write_timestamp(void)
{
    time_t     t  = time(NULL);
    struct tm *tm = localtime(&t);
    char       buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
    fprintf(log_fp, "[%s] ", buf);
}

/* ── Public functions ────────────────────────────────────────────────── */

void logger_init(const char *filepath)
{
    log_fp = fopen(filepath, "a");   /* append mode — survives restarts */
    if (!log_fp) {
        perror("logger: fopen failed");
    }
}

/*
 * logger_log — called by every worker thread after serving a request.
 * The mutex ensures only one thread writes at a time → no garbled lines.
 */
void logger_log(int thread_id, const char *method,
                const char *path, int status_code)
{
    if (!log_fp) return;

    pthread_mutex_lock(&log_lock);   /* ← acquire lock */

    write_timestamp();
    fprintf(log_fp, "[Thread-%d] %s %s %d\n",
            thread_id, method, path, status_code);
    fflush(log_fp);

    pthread_mutex_unlock(&log_lock); /* ← release lock */
}

/*
 * logger_info — log a plain text message (server start, shutdown, etc.)
 */
void logger_info(const char *msg)
{
    if (!log_fp) return;

    pthread_mutex_lock(&log_lock);
    write_timestamp();
    fprintf(log_fp, "[INFO] %s\n", msg);
    fflush(log_fp);
    pthread_mutex_unlock(&log_lock);
}

void logger_close(void)
{
    if (log_fp) {
        fclose(log_fp);
        log_fp = NULL;
    }
}
