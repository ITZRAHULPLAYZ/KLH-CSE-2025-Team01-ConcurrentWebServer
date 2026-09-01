#ifndef LOGGER_H
#define LOGGER_H

/* Call once at startup */
void logger_init(const char *filepath);

/* Log a completed request — thread-safe */
void logger_log(int thread_id, const char *method,
                const char *path, int status_code);

/* Log a plain message (errors, events) — thread-safe */
void logger_info(const char *msg);

/* Call once at shutdown */
void logger_close(void);

#endif /* LOGGER_H */
