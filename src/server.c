/*
 * server.c
 * --------
 * Concurrent Web Server — Main File
 *
 * OS Concepts demonstrated:
 *   - TCP sockets (socket, bind, listen, accept)
 *   - POSIX threads via thread pool (thread_pool.c)
 *   - Mutex + condition variables for shared queue
 *   - mmap() for efficient file serving
 *   - Signals: SIGINT (shutdown), SIGHUP (reload), SIGUSR1 (stats)
 *   - /proc filesystem for resource monitoring
 *
 * Build:  make
 * Run:    ./server [port]
 * Test:   curl http://localhost:8080/index.html
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

#include "thread_pool.h"
#include "logger.h"

/* ── Config ──────────────────────────────────────────────────────────── */
#define DEFAULT_PORT   8080
#define WWW_ROOT       "data/www"      /* where HTML files live          */
#define LOG_FILE       "server.log"
#define BUF_SIZE       4096

/* ── Global shutdown flag (set by signal handler) ───────────────────── */
static volatile int running = 1;

/* ── Shared connection queue (defined in thread_pool.h) ─────────────── */
static ConnQueue queue;

/* ═══════════════════════════════════════════════════════════════════════
 * /proc RESOURCE MONITOR
 * Reads /proc/self/status to get memory usage.
 * Called when SIGUSR1 is received.
 * ═══════════════════════════════════════════════════════════════════════ */
static void print_stats(void)
{
    printf("\n─── Server Stats ───────────────────\n");

    /* Active queue size */
    pthread_mutex_lock(&queue.lock);
    printf("  Pending connections in queue : %d\n", queue.count);
    pthread_mutex_unlock(&queue.lock);

    /* Memory from /proc */
    FILE *fp = fopen("/proc/self/status", "r");
    if (fp) {
        char line[128];
        while (fgets(line, sizeof(line), fp)) {
            /* VmRSS = actual physical RAM used by this process */
            if (strncmp(line, "VmRSS:", 6) == 0) {
                printf("  Memory (VmRSS)               : %s", line + 7);
                break;
            }
        }
        fclose(fp);
    }

    /* Open file descriptors from /proc */
    FILE *fd_fp = fopen("/proc/self/status", "r");
    if (fd_fp) {
        char line[128];
        while (fgets(line, sizeof(line), fd_fp)) {
            if (strncmp(line, "FDSize:", 7) == 0) {
                printf("  FD Table Size                : %s", line + 8);
                break;
            }
        }
        fclose(fd_fp);
    }

    printf("────────────────────────────────────\n\n");
    logger_info("SIGUSR1: stats printed to stdout");
}

/* ═══════════════════════════════════════════════════════════════════════
 * SIGNAL HANDLERS
 * ═══════════════════════════════════════════════════════════════════════ */

/* SIGINT / SIGTERM — Ctrl+C or kill: graceful shutdown */
static void handle_sigint(int sig)
{
    (void)sig;
    printf("\n[Signal] SIGINT received — shutting down...\n");
    running = 0;

    /* Wake blocked workers so they can exit */
    pthread_mutex_lock(&queue.lock);
    pthread_cond_broadcast(&queue.not_empty);
    pthread_mutex_unlock(&queue.lock);
}

/* SIGHUP — typically used to reload config; we just log it */
static void handle_sighup(int sig)
{
    (void)sig;
    printf("[Signal] SIGHUP received — config reloaded (no-op)\n");
    logger_info("SIGHUP: config reload signal received");
}

/* SIGUSR1 — dump stats */
static void handle_sigusr1(int sig)
{
    (void)sig;
    print_stats();
}

/* SIGPIPE — ignore: prevents crash when client disconnects early */
static void handle_sigpipe(int sig)
{
    (void)sig;
    /* do nothing */
}

static void register_signals(void)
{
    signal(SIGINT,  handle_sigint);
    signal(SIGTERM, handle_sigint);
    signal(SIGHUP,  handle_sighup);
    signal(SIGUSR1, handle_sigusr1);
    signal(SIGPIPE, handle_sigpipe);
}

/* ═══════════════════════════════════════════════════════════════════════
 * MIME TYPE DETECTION
 * Returns the correct Content-Type based on file extension.
 * ═══════════════════════════════════════════════════════════════════════ */
static const char *get_mime_type(const char *path)
{
    const char *ext = strrchr(path, '.');   /* find last '.' */
    if (!ext)
        return "application/octet-stream";

    if (strcmp(ext, ".html") == 0 || strcmp(ext, ".htm") == 0)
        return "text/html";
    if (strcmp(ext, ".txt") == 0)
        return "text/plain";
    if (strcmp(ext, ".css") == 0)
        return "text/css";
    if (strcmp(ext, ".js") == 0)
        return "application/javascript";
    if (strcmp(ext, ".pdf") == 0)
        return "application/pdf";
    if (strcmp(ext, ".png") == 0)
        return "image/png";
    if (strcmp(ext, ".jpg") == 0 || strcmp(ext, ".jpeg") == 0)
        return "image/jpeg";

    return "application/octet-stream";
}

/* ═══════════════════════════════════════════════════════════════════════
 * HTTP RESPONSE HELPERS
 * ═══════════════════════════════════════════════════════════════════════ */
static void send_404(int client_fd)
{
    const char *body =
        "<html><body><h1>404 Not Found</h1>"
        "<p>The requested file was not found.</p></body></html>";
    char header[256];
    snprintf(header, sizeof(header),
             "HTTP/1.0 404 Not Found\r\n"
             "Content-Type: text/html\r\n"
             "Content-Length: %zu\r\n"
             "\r\n", strlen(body));
    send(client_fd, header, strlen(header), 0);
    send(client_fd, body,   strlen(body),   0);
}

static void send_403(int client_fd)
{
    const char *body =
        "<html><body><h1>403 Forbidden</h1></body></html>";
    char header[256];
    snprintf(header, sizeof(header),
             "HTTP/1.0 403 Forbidden\r\n"
             "Content-Type: text/html\r\n"
             "Content-Length: %zu\r\n"
             "\r\n", strlen(body));
    send(client_fd, header, strlen(header), 0);
    send(client_fd, body,   strlen(body),   0);
}

/* ═══════════════════════════════════════════════════════════════════════
 * SERVE FILE WITH mmap()
 *
 * Instead of read()-ing into a buffer, we memory-map the file.
 * The OS maps the file pages directly into our address space — no
 * intermediate copying. This is more efficient for large files.
 *
 * OS Concept: Virtual Memory, Memory-Mapped Files (mmap syscall)
 * ═══════════════════════════════════════════════════════════════════════ */
static void serve_file(int client_fd, const char *filepath,
                       const char *method, int thread_id)
{
    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        send_404(client_fd);
        logger_log(thread_id, method, filepath, 404);
        return;
    }

    struct stat st;
    fstat(fd, &st);   /* get file size */

    /* mmap the file — MAP_PRIVATE: changes don't affect the file */
    void *data = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);   /* fd can be closed immediately after mmap */

    if (data == MAP_FAILED) {
        send_404(client_fd);
        logger_log(thread_id, method, filepath, 404);
        return;
    }

    /* Send HTTP response header */
    char header[512];
    snprintf(header, sizeof(header),
             "HTTP/1.0 200 OK\r\n"
             "Content-Type: %s\r\n"
             "Content-Length: %ld\r\n"
             "\r\n",
             get_mime_type(filepath), st.st_size);
    send(client_fd, header, strlen(header), 0);

    /* Send the file content directly from the mmap'd region */
    send(client_fd, data, st.st_size, 0);

    /* Unmap — release the virtual memory region */
    munmap(data, st.st_size);

    logger_log(thread_id, method, filepath, 200);
}

/* ═══════════════════════════════════════════════════════════════════════
 * handle_client — called by each worker thread
 *
 * Parses the HTTP request and serves the appropriate file.
 * This function runs in one of the 4 worker threads.
 * ═══════════════════════════════════════════════════════════════════════ */
void handle_client(int client_fd, int thread_id)
{
    char buf[BUF_SIZE] = {0};

    /* Receive the HTTP request */
    ssize_t n = recv(client_fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) return;

    /* ── Parse: "GET /path HTTP/1.x" ─────────────────────────── */
    char method[16] = {0};
    char path[256]  = {0};

    sscanf(buf, "%15s %255s", method, path);

    /* Only handle GET for now */
    if (strcmp(method, "GET") != 0) {
        const char *resp = "HTTP/1.0 405 Method Not Allowed\r\n\r\n";
        send(client_fd, resp, strlen(resp), 0);
        return;
    }

    /* "/" → serve index.html */
    if (strcmp(path, "/") == 0)
        strcpy(path, "/index.html");

    /* ── Security: block path traversal (../../etc/passwd) ──── */
    if (strstr(path, "..")) {
        send_403(client_fd);
        logger_log(thread_id, method, path, 403);
        return;
    }

    /* ── Build full filesystem path ─────────────────────────── */
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s%s", WWW_ROOT, path);

    /* Serve the file */
    serve_file(client_fd, filepath, method, thread_id);
}

/* ═══════════════════════════════════════════════════════════════════════
 * MAIN — Entry point
 * ═══════════════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[])
{
    int port = (argc > 1) ? atoi(argv[1]) : DEFAULT_PORT;
    int pool_size = (argc > 2) ? atoi(argv[2]) : 64;

    /* ── Setup logger ───────────────────────────────────────── */
    logger_init(LOG_FILE);
    logger_info("Server starting up");

    /* ── Register signal handlers ───────────────────────────── */
    register_signals();

    /* ── Create TCP server socket ───────────────────────────── */
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    /* Allow reuse of port immediately after server restart */
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;   /* listen on all interfaces */

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(server_fd, 1024) < 0) {
        perror("listen");
        return 1;
    }

    printf("╔══════════════════════════════════════╗\n");
    printf("║  Concurrent Web Server               ║\n");
    printf("║  Port     : %-5d                    ║\n", port);
    printf("║  Threads  : %-5d                    ║\n", pool_size);
    printf("║  WWW Root : %-25s║\n", WWW_ROOT);
    printf("╚══════════════════════════════════════╝\n");
    printf("  Press Ctrl+C to stop\n");
    printf("  Send SIGUSR1 (kill -USR1 %d) for stats\n\n", getpid());

    /* ── Init connection queue ──────────────────────────────── */
    queue_init(&queue);

    /* ── Start worker threads ───────────────────────────────── */
    thread_pool_init(&queue, pool_size);

    /* ── Accept loop (main thread) ──────────────────────────── */
    while (running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd,
                               (struct sockaddr *)&client_addr,
                               &client_len);

        if (client_fd < 0) {
            if (!running) break;   /* accept interrupted by signal */
            perror("accept");
            continue;
        }

        printf("[Main] New connection from %s\n",
               inet_ntoa(client_addr.sin_addr));

        /* Hand off client to the thread pool queue */
        queue_push(&queue, client_fd);
    }

    /* ── Graceful shutdown ──────────────────────────────────── */
    printf("\n[Main] Shutting down...\n");
    logger_info("Server shutting down");

    thread_pool_shutdown();
    queue_destroy(&queue);
    close(server_fd);
    logger_close();

    printf("[Main] All threads joined. Goodbye!\n");
    return 0;
}
