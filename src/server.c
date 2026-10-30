/*
 * server.c
 * --------
 * Concurrent Web Server — Main File
 *
 * OS Concepts demonstrated:
 *   - TCP sockets (socket, bind, listen, accept)
 *   - Elastic POSIX thread pool (thread_pool.c)
 *     · starts with MIN_THREADS, scales to MAX_THREADS on demand
 *     · idle excess threads retire after IDLE_TIMEOUT seconds
 *     · dynamic live auto-resize (/api/set_pool)
 *   - Mutex + POSIX counting semaphores (sem_t) for shared queue (producer-consumer)
 *   - mmap() for efficient zero-copy file serving
 *   - POSIX signals: SIGINT (shutdown), SIGHUP (config reload from server.conf), SIGUSR1 (stats)
 *   - /proc filesystem for live resource monitoring
 *   - /api/stats JSON endpoint for live telemetry dashboard
 *
 * Build:  make
 * Run:    ./server [port] [min_threads] [max_threads]
 * Test:   curl http://localhost:8080/api/stats
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>

#include "thread_pool.h"
#include "logger.h"

/* ── Config ───────────────────────────────────────────────────────── */
#define DEFAULT_PORT   8080
#define WWW_ROOT       "data/www"
#define LOG_FILE       "server.log"
#define CONFIG_FILE    "server.conf"
#define BUF_SIZE       4096

/* ── Globals ──────────────────────────────────────────────────────── */
static volatile int running = 1;
static ConnQueue    queue;
static int          cfg_port        = DEFAULT_PORT;
static int          cfg_pool_size   = DEFAULT_POOL_SIZE;
static char         cfg_www_root[256] = WWW_ROOT;
static char         cfg_log_file[256] = LOG_FILE;

/* Request counter — updated with GCC atomic to avoid a separate mutex */
static volatile long total_requests = 0;

/* Server start time for uptime calculation */
static time_t server_start_time;

/* ══════════════════════════════════════════════════════════════════════
 * CONFIGURATION LOADER — reloaded at runtime on SIGHUP
 * ══════════════════════════════════════════════════════════════════════ */
static void load_config(const char *filepath)
{
    FILE *fp = fopen(filepath, "r");
    if (!fp) return;

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') continue;
        char key[64] = {0}, val[192] = {0};
        if (sscanf(line, "%63[^=]=%191s", key, val) == 2) {
            if (strcmp(key, "port") == 0) cfg_port = atoi(val);
            else if (strcmp(key, "pool_size") == 0 || strcmp(key, "min_threads") == 0) cfg_pool_size = atoi(val);
            else if (strcmp(key, "www_root") == 0) strncpy(cfg_www_root, val, sizeof(cfg_www_root) - 1);
            else if (strcmp(key, "log_file") == 0) strncpy(cfg_log_file, val, sizeof(cfg_log_file) - 1);
        }
    }
    fclose(fp);
}

/* ══════════════════════════════════════════════════════════════════════
 * /proc RESOURCE MONITOR — prints on SIGUSR1
 * ══════════════════════════════════════════════════════════════════════ */
static void print_stats(void)
{
    PoolStats ps = thread_pool_stats();

    printf("\n─── Server Stats ───────────────────────\n");
    printf("  Pool size         : %d threads\n", ps.pool_size);
    printf("  Active threads    : %d\n", ps.active);
    printf("  Busy threads      : %d\n", ps.busy);
    printf("  Idle threads      : %d\n", ps.idle);
    printf("  Queue depth       : %d\n", ps.queue_depth);
    printf("  Total requests    : %ld\n", total_requests);
    printf("  Uptime            : %ld s\n", (long)(time(NULL) - server_start_time));

    FILE *fp = fopen("/proc/self/status", "r");
    if (fp) {
        char line[128];
        while (fgets(line, sizeof(line), fp)) {
            if (strncmp(line, "VmRSS:", 6) == 0) {
                printf("  Memory (VmRSS)    : %s", line + 7);
                break;
            }
        }
        fclose(fp);
    }
    printf("────────────────────────────────────────\n\n");
    logger_info("SIGUSR1: stats printed");
}

/* ══════════════════════════════════════════════════════════════════════
 * SIGNAL HANDLERS
 * ══════════════════════════════════════════════════════════════════════ */
static void handle_sigint(int sig)
{
    (void)sig;
    printf("\n[Signal] SIGINT — shutting down...\n");
    running = 0;
    pthread_mutex_lock(&queue.lock);
    pthread_cond_broadcast(&queue.not_empty);
    pthread_mutex_unlock(&queue.lock);
}

static void handle_sighup(int sig)
{
    (void)sig;
    int old_pool = cfg_pool_size;
    load_config(CONFIG_FILE);
    printf("\n[Signal] SIGHUP — reloaded config from %s (pool_size: %d -> %d)\n",
           CONFIG_FILE, old_pool, cfg_pool_size);
    char msg[128];
    snprintf(msg, sizeof(msg), "SIGHUP: config reloaded from %s (pool_size=%d)",
             CONFIG_FILE, cfg_pool_size);
    logger_info(msg);

    if (cfg_pool_size != old_pool) {
        thread_pool_set_size(cfg_pool_size);
    }
}

static void handle_sigusr1(int sig) { (void)sig; print_stats(); }
static void handle_sigpipe(int sig) { (void)sig; }

static void register_signals(void)
{
    signal(SIGINT,  handle_sigint);
    signal(SIGTERM, handle_sigint);
    signal(SIGHUP,  handle_sighup);
    signal(SIGUSR1, handle_sigusr1);
    signal(SIGPIPE, handle_sigpipe);
}

/* ══════════════════════════════════════════════════════════════════════
 * MIME TYPE
 * ══════════════════════════════════════════════════════════════════════ */
static const char *get_mime(const char *path)
{
    const char *e = strrchr(path, '.');
    if (!e) return "application/octet-stream";
    if (!strcmp(e, ".html") || !strcmp(e, ".htm")) return "text/html";
    if (!strcmp(e, ".css"))                         return "text/css";
    if (!strcmp(e, ".js"))                          return "application/javascript";
    if (!strcmp(e, ".json"))                        return "application/json";
    if (!strcmp(e, ".txt"))                         return "text/plain";
    if (!strcmp(e, ".png"))                         return "image/png";
    if (!strcmp(e, ".jpg") || !strcmp(e, ".jpeg")) return "image/jpeg";
    if (!strcmp(e, ".pdf"))                         return "application/pdf";
    return "application/octet-stream";
}

/* ══════════════════════════════════════════════════════════════════════
 * HTTP RESPONSE HELPERS
 * ══════════════════════════════════════════════════════════════════════ */
static void send_response(int fd, int code, const char *ctype,
                          const char *body, size_t blen)
{
    const char *status =
        code == 200 ? "200 OK" :
        code == 403 ? "403 Forbidden" :
        code == 404 ? "404 Not Found" :
        code == 405 ? "405 Method Not Allowed" : "500 Error";

    char header[512];
    int hlen = snprintf(header, sizeof(header),
        "HTTP/1.0 %s\r\n"
        "Content-Type: %s\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Content-Length: %zu\r\n"
        "\r\n",
        status, ctype, blen);
    send(fd, header, hlen, 0);
    if (body && blen) send(fd, body, blen, 0);
}

static void send_404(int fd)
{
    const char *b = "<html><body><h1>404 Not Found</h1></body></html>";
    send_response(fd, 404, "text/html", b, strlen(b));
}

static void send_403(int fd)
{
    const char *b = "<html><body><h1>403 Forbidden</h1></body></html>";
    send_response(fd, 403, "text/html", b, strlen(b));
}

/* ══════════════════════════════════════════════════════════════════════
 * /api/stats — JSON endpoint for the live dashboard
 * ══════════════════════════════════════════════════════════════════════ */
static void send_stats_json(int fd, int caller_tid)
{
    PoolStats ps = thread_pool_stats();
    long uptime  = (long)(time(NULL) - server_start_time);
    long reqs    = total_requests;

    ThreadInfo tinfo[MAX_POOL_SIZE];
    int nthreads = thread_pool_get_threads(tinfo, MAX_POOL_SIZE);

    int busy_count = 0;
    for (int i = 0; i < nthreads; i++) {
        if (tinfo[i].tid != caller_tid && tinfo[i].busy) {
            busy_count++;
        }
    }

    size_t cap = 65536;
    char *body = malloc(cap);
    if (!body) return;

    int offset = snprintf(body, cap,
        "{"
        "\"pool_size\":%d,"
        "\"busy_threads\":%d,"
        "\"idle_threads\":%d,"
        "\"queue_depth\":%d,"
        "\"total_requests\":%ld,"
        "\"uptime_seconds\":%ld,"
        "\"threads\":[",
        ps.pool_size, busy_count, ps.active - busy_count,
        ps.queue_depth, reqs, uptime);

    for (int i = 0; i < nthreads; i++) {
        int is_busy = (tinfo[i].tid == caller_tid) ? 0 : tinfo[i].busy;
        offset += snprintf(body + offset, cap - offset,
            "%s{\"id\":%d,\"busy\":%d,\"reqs\":%ld}",
            (i > 0) ? "," : "",
            tinfo[i].tid,
            is_busy,
            tinfo[i].requests_count);
        if ((size_t)offset >= cap - 128) break;
    }

    offset += snprintf(body + offset, cap - offset, "]}");

    send_response(fd, 200, "application/json", body, offset);
    free(body);
}

/* ══════════════════════════════════════════════════════════════════════
 * SERVE FILE with mmap()
 *
 * OS Concept: Virtual Memory / Memory-Mapped Files.
 * The file is mapped directly into our address space — no intermediate
 * heap copy. The kernel shares the page-cache pages with us.
 * ══════════════════════════════════════════════════════════════════════ */
static void serve_file(int client_fd, const char *filepath,
                       const char *method, int thread_id)
{
    int fd = open(filepath, O_RDONLY);
    if (fd < 0) { send_404(client_fd); logger_log(thread_id, method, filepath, 404); return; }

    struct stat st;
    fstat(fd, &st);

    void *data = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        send_404(client_fd);
        logger_log(thread_id, method, filepath, 404);
        return;
    }

    send_response(client_fd, 200, get_mime(filepath), data, st.st_size);
    munmap(data, st.st_size);

    /* Atomically increment request counter */
    __sync_fetch_and_add(&total_requests, 1);
    logger_log(thread_id, method, filepath, 200);
}

/* ══════════════════════════════════════════════════════════════════════
 * handle_client — called by each worker thread
 *
 * Parses the HTTP request, routes /api/stats to the JSON handler,
 * /api/set_pool for dynamic resize, /api/work for simulated backend work,
 * otherwise serves a file from WWW_ROOT using mmap().
 * ══════════════════════════════════════════════════════════════════════ */
void handle_client(int client_fd, int thread_id)
{
    char buf[BUF_SIZE] = {0};

    ssize_t n = recv(client_fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) return;

    char method[16] = {0};
    char path[256]  = {0};
    sscanf(buf, "%15s %255s", method, path);

    /* Only GET supported */
    if (strcmp(method, "GET") != 0) {
        const char *r = "HTTP/1.0 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
        send(client_fd, r, strlen(r), 0);
        return;
    }

    /* "/" → index.html */
    if (strcmp(path, "/") == 0) strcpy(path, "/index.html");

    /* ── Route: /api/stats ── */
    if (strcmp(path, "/api/stats") == 0) {
        send_stats_json(client_fd, thread_id);
        logger_log(thread_id, method, "/api/stats", 200);
        return;
    }

    /* ── Route: /api/set_pool?size=N (live auto-resize without restart) ── */
    if (strncmp(path, "/api/set_pool", 13) == 0) {
        int new_size = 4;
        char *q = strstr(path, "size=");
        if (q) new_size = atoi(q + 5);
        if (new_size < 1) new_size = 1;
        if (new_size > 1024) new_size = 1024;

        int current = thread_pool_set_size(new_size);

        char body[256];
        int blen = snprintf(body, sizeof(body),
            "{\"status\":\"ok\",\"pool_size\":%d,\"min_threads\":%d}",
            current, new_size);
        send_response(client_fd, 200, "application/json", body, blen);
        logger_log(thread_id, method, path, 200);
        return;
    }

    /* ── Route: /api/work?ms=N (simulates academic portal workload) ── */
    if (strncmp(path, "/api/work", 9) == 0) {
        int ms = 25;
        char *q = strstr(path, "ms=");
        if (q) ms = atoi(q + 3);
        if (ms < 0) ms = 0;
        if (ms > 2000) ms = 2000;

        if (ms > 0) {
            usleep(ms * 1000);
        }

        __sync_fetch_and_add(&total_requests, 1);
        char body[128];
        int blen = snprintf(body, sizeof(body),
            "{\"status\":\"ok\",\"simulated_ms\":%d}", ms);
        send_response(client_fd, 200, "application/json", body, blen);
        logger_log(thread_id, method, path, 200);
        return;
    }

    /* ── Security: block path traversal ── */
    if (strstr(path, "..")) {
        send_403(client_fd);
        logger_log(thread_id, method, path, 403);
        return;
    }

    /* Build filesystem path and serve */
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s%s", cfg_www_root, path);
    serve_file(client_fd, filepath, method, thread_id);
}

/* ══════════════════════════════════════════════════════════════════════
 * MAIN
 * ══════════════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[])
{
    load_config(CONFIG_FILE);

    int port      = (argc > 1) ? atoi(argv[1]) : cfg_port;
    int pool_size = (argc > 2) ? atoi(argv[2]) : cfg_pool_size;

    server_start_time = time(NULL);

    logger_init(cfg_log_file);
    logger_info("Server starting");
    register_signals();

    /* ── TCP socket setup ── */
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(server_fd, 1024) < 0) { perror("listen"); return 1; }

    printf("┌────────────────────────────────────────┐\n");
    printf("│  KLH Concurrent Web Server             │\n");
    printf("│  Port        : %-5d                   │\n", port);
    printf("│  Worker Pool : %-5d persistent threads│\n", pool_size);
    printf("│  Queue Cap   : %-5d pending slots     │\n", QUEUE_SIZE);
    printf("│  WWW Root    : %-24s│\n", cfg_www_root);
    printf("│  Config File : %-24s│\n", CONFIG_FILE);
    printf("│  Stats API   : /api/stats              │\n");
    printf("└────────────────────────────────────────┘\n");
    printf("  Ctrl+C to stop  │  SIGHUP (kill -HUP %d) to reload config\n", getpid());
    printf("                  │  SIGUSR1 (kill -USR1 %d) for stats\n\n", getpid());

    /* ── Init queue and worker thread pool ── */
    queue_init(&queue);
    thread_pool_init(&queue, pool_size);

    /* ── Accept loop (main thread) ── */
    while (running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd,
                               (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            if (!running) break;
            perror("accept");
            continue;
        }

        printf("[Main] Connection from %s\n", inet_ntoa(client_addr.sin_addr));
        queue_push(&queue, client_fd);
    }

    /* ── Graceful shutdown ── */
    printf("\n[Main] Shutting down...\n");
    logger_info("Server shutting down");

    thread_pool_shutdown();
    queue_destroy(&queue);
    close(server_fd);
    logger_close();

    printf("[Main] Done. Served %ld requests total.\n", total_requests);
    return 0;
}
