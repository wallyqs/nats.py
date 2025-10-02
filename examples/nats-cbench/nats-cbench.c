/*
 * NATS C Benchmark Tool
 * 
 * High-performance benchmark tool using the NATS C client directly.
 * Usage: nats-cbench pub <subject> --size <size> --msgs <count> --clients <num>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>
#include <locale.h>

#include "nats.h"

typedef struct {
    int client_id;
    const char *subject;
    const char *server_url;
    char *payload;
    int payload_size;
    int msg_count;
    
    // Results
    int messages_sent;
    int errors;
    double start_time;
    double end_time;
    double msgs_per_sec;
    double bytes_per_sec;
} client_context_t;

typedef struct {
    const char *command;
    const char *subject;
    const char *server_url;
    int msg_size;
    int msg_count;
    int client_count;
    int no_progress;
} bench_config_t;

// Global config
static bench_config_t g_config = {
    .command = NULL,
    .subject = NULL,
    .server_url = "nats://localhost:4222",
    .msg_size = 128,
    .msg_count = 100000,
    .client_count = 1,
    .no_progress = 0
};

// Function prototypes
static void usage(const char *prog);
static int parse_args(int argc, char *argv[]);
static double get_time_in_seconds(void);
static char* format_bytes(long bytes, char *buf, size_t buf_size);
static char* format_number(long num, char *buf, size_t buf_size);
static void* publish_worker(void *arg);
static int run_publish_benchmark(void);

double get_time_in_seconds(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

char* format_bytes(long bytes, char *buf, size_t buf_size) {
    const char *units[] = {"B", "KB", "MB", "GB", "TB"};
    int unit = 0;
    double value = (double)bytes;
    
    while (value >= 1024.0 && unit < 4) {
        value /= 1024.0;
        unit++;
    }
    
    if (unit == 0) {
        snprintf(buf, buf_size, "%.0f%s", value, units[unit]);
    } else {
        snprintf(buf, buf_size, "%.1f%s", value, units[unit]);
    }
    
    return buf;
}

char* format_number(long num, char *buf, size_t buf_size) {
    char temp[64];
    snprintf(temp, sizeof(temp), "%ld", num);
    
    int len = strlen(temp);
    int comma_count = (len - 1) / 3;
    int new_len = len + comma_count;
    
    if ((size_t)new_len >= buf_size) {
        snprintf(buf, buf_size, "%ld", num);
        return buf;
    }
    
    buf[new_len] = '\0';
    
    int src = len - 1;
    int dst = new_len - 1;
    int digits = 0;
    
    while (src >= 0) {
        if (digits == 3) {
            buf[dst--] = ',';
            digits = 0;
        }
        buf[dst--] = temp[src--];
        digits++;
    }
    
    return buf;
}

void usage(const char *prog) {
    printf("NATS C Benchmark Tool\n\n");
    printf("Usage: %s <command> <subject> [options]\n\n", prog);
    printf("Commands:\n");
    printf("  pub <subject>    Run publish benchmark\n\n");
    printf("Options:\n");
    printf("  --server, -s <url>    NATS server URL (default: %s)\n", g_config.server_url);
    printf("  --size <size>         Message size in bytes (default: %d)\n", g_config.msg_size);
    printf("  --msgs <count>        Total number of messages (default: %d)\n", g_config.msg_count);
    printf("  --clients <count>     Number of concurrent clients (default: %d)\n", g_config.client_count);
    printf("  --no-progress         Disable progress output\n");
    printf("  --help, -h            Show this help\n\n");
    printf("Examples:\n");
    printf("  %s pub foo --size 0 --msgs 1000000 --clients 1\n", prog);
    printf("  %s pub test.subject --size 1024 --msgs 100000 --clients 3\n", prog);
}

int parse_args(int argc, char *argv[]) {
    static struct option long_options[] = {
        {"server",      required_argument, 0, 's'},
        {"size",        required_argument, 0, 'z'},
        {"msgs",        required_argument, 0, 'm'},
        {"clients",     required_argument, 0, 'c'},
        {"no-progress", no_argument,       0, 'n'},
        {"help",        no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    if (argc < 3) {
        usage(argv[0]);
        return -1;
    }
    
    // Parse command and subject
    g_config.command = argv[1];
    g_config.subject = argv[2];
    
    if (strcmp(g_config.command, "pub") != 0) {
        fprintf(stderr, "Error: Unknown command '%s'\n", g_config.command);
        return -1;
    }
    
    // Parse options
    int c;
    int option_index = 0;
    optind = 3; // Start parsing from the third argument
    
    while ((c = getopt_long(argc, argv, "s:z:m:c:nh", long_options, &option_index)) != -1) {
        switch (c) {
            case 's':
                g_config.server_url = optarg;
                break;
            case 'z':
                g_config.msg_size = atoi(optarg);
                if (g_config.msg_size < 0) {
                    fprintf(stderr, "Error: Invalid message size: %s\n", optarg);
                    return -1;
                }
                break;
            case 'm':
                g_config.msg_count = atoi(optarg);
                if (g_config.msg_count <= 0) {
                    fprintf(stderr, "Error: Invalid message count: %s\n", optarg);
                    return -1;
                }
                break;
            case 'c':
                g_config.client_count = atoi(optarg);
                if (g_config.client_count <= 0) {
                    fprintf(stderr, "Error: Invalid client count: %s\n", optarg);
                    return -1;
                }
                break;
            case 'n':
                g_config.no_progress = 1;
                break;
            case 'h':
                usage(argv[0]);
                return 1;
            case '?':
                return -1;
            default:
                abort();
        }
    }
    
    return 0;
}

void* publish_worker(void *arg) {
    client_context_t *ctx = (client_context_t*)arg;
    natsConnection *nc = NULL;
    natsStatus s;
    
    // Connect to NATS
    s = natsConnection_ConnectTo(&nc, ctx->server_url);
    if (s != NATS_OK) {
        fprintf(stderr, "Client %d: Failed to connect: %s\n", 
                ctx->client_id, natsStatus_GetText(s));
        ctx->errors = 1;
        return NULL;
    }
    
    ctx->start_time = get_time_in_seconds();
    
    // Publish messages
    for (int i = 0; i < ctx->msg_count; i++) {
        s = natsConnection_Publish(nc, ctx->subject, ctx->payload, ctx->payload_size);
        if (s != NATS_OK) {
            ctx->errors++;
        } else {
            ctx->messages_sent++;
        }
    }
    
    // Flush to ensure all messages are sent
    s = natsConnection_Flush(nc);
    if (s != NATS_OK) {
        fprintf(stderr, "Client %d: Failed to flush: %s\n", 
                ctx->client_id, natsStatus_GetText(s));
    }
    
    ctx->end_time = get_time_in_seconds();
    
    // Calculate statistics
    double total_time = ctx->end_time - ctx->start_time;
    if (total_time > 0) {
        ctx->msgs_per_sec = ctx->messages_sent / total_time;
        ctx->bytes_per_sec = (ctx->messages_sent * ctx->payload_size) / total_time;
    }
    
    // Close connection
    natsConnection_Destroy(nc);
    
    return NULL;
}

int run_publish_benchmark(void) {
    pthread_t *threads;
    client_context_t *contexts;
    char *payload;
    char buf[64];
    
    // Allocate payload
    payload = malloc(g_config.msg_size);
    if (!payload) {
        fprintf(stderr, "Error: Failed to allocate payload\n");
        return -1;
    }
    memset(payload, 'x', g_config.msg_size);
    
    // Calculate messages per client
    int msgs_per_client = g_config.msg_count / g_config.client_count;
    int remaining_msgs = g_config.msg_count % g_config.client_count;
    
    // Allocate threads and contexts
    threads = malloc(g_config.client_count * sizeof(pthread_t));
    contexts = malloc(g_config.client_count * sizeof(client_context_t));
    
    if (!threads || !contexts) {
        fprintf(stderr, "Error: Failed to allocate memory\n");
        free(payload);
        return -1;
    }
    
    // Print benchmark info
    char num_buf[64];
    printf("Running publish benchmark:\n");
    printf("  Subject: %s\n", g_config.subject);
    printf("  Clients: %s\n", format_number(g_config.client_count, num_buf, sizeof(num_buf)));
    printf("  Total messages: %s\n", format_number(g_config.msg_count, buf, sizeof(buf)));
    if (remaining_msgs > 0) {
        printf("  Messages per client: %s (%d clients get +1)\n", format_number(msgs_per_client, num_buf, sizeof(num_buf)), remaining_msgs);
    } else {
        printf("  Messages per client: %s\n", format_number(msgs_per_client, num_buf, sizeof(num_buf)));
    }
    printf("  Message size: %s\n", format_bytes(g_config.msg_size, buf, sizeof(buf)));
    printf("  Total data: %s\n", format_bytes((long)g_config.msg_size * g_config.msg_count, buf, sizeof(buf)));
    printf("\n");
    
    // Initialize contexts
    for (int i = 0; i < g_config.client_count; i++) {
        contexts[i].client_id = i;
        contexts[i].subject = g_config.subject;
        contexts[i].server_url = g_config.server_url;
        contexts[i].payload = payload;
        contexts[i].payload_size = g_config.msg_size;
        contexts[i].msg_count = msgs_per_client;
        if (i < remaining_msgs) {
            contexts[i].msg_count++;
        }
        contexts[i].messages_sent = 0;
        contexts[i].errors = 0;
        contexts[i].msgs_per_sec = 0;
        contexts[i].bytes_per_sec = 0;
    }
    
    // Start benchmark
    double bench_start = get_time_in_seconds();
    
    // Create threads
    for (int i = 0; i < g_config.client_count; i++) {
        if (pthread_create(&threads[i], NULL, publish_worker, &contexts[i]) != 0) {
            fprintf(stderr, "Error: Failed to create thread %d\n", i);
            return -1;
        }
    }
    
    // Wait for threads to complete
    for (int i = 0; i < g_config.client_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    double bench_end = get_time_in_seconds();
    double total_time = bench_end - bench_start;
    
    // Calculate aggregate statistics
    int total_msgs_sent = 0;
    long total_bytes_sent = 0;
    int total_errors = 0;
    double sum_rates = 0;
    double min_rate = 1e9, max_rate = 0;
    
    for (int i = 0; i < g_config.client_count; i++) {
        total_msgs_sent += contexts[i].messages_sent;
        total_bytes_sent += contexts[i].messages_sent * contexts[i].payload_size;
        total_errors += contexts[i].errors;
        
        if (contexts[i].msgs_per_sec > 0) {
            sum_rates += contexts[i].msgs_per_sec;
            if (contexts[i].msgs_per_sec < min_rate) min_rate = contexts[i].msgs_per_sec;
            if (contexts[i].msgs_per_sec > max_rate) max_rate = contexts[i].msgs_per_sec;
        }
    }
    
    double overall_msgs_per_sec = total_msgs_sent / total_time;
    double overall_bytes_per_sec = total_bytes_sent / total_time;
    
    // Print results in nats bench format
    char rate_buf[64], msg_buf[64], err_buf[64];
    printf("Pub stats: %s msgs/sec ~ %s/sec\n", 
           format_number((long)overall_msgs_per_sec, rate_buf, sizeof(rate_buf)),
           format_bytes((long)overall_bytes_per_sec, buf, sizeof(buf)));
    
    // Print per-connection stats
    for (int i = 0; i < g_config.client_count; i++) {
        printf(" [%d] %s msgs/sec ~ %s/sec (%s msgs)\n",
               i + 1,
               format_number((long)contexts[i].msgs_per_sec, rate_buf, sizeof(rate_buf)),
               format_bytes((long)contexts[i].bytes_per_sec, buf, sizeof(buf)),
               format_number(contexts[i].messages_sent, msg_buf, sizeof(msg_buf)));
    }
    
    // Print summary statistics
    if (g_config.client_count > 1) {
        double avg_rate = sum_rates / g_config.client_count;
        double stddev = 0;
        for (int i = 0; i < g_config.client_count; i++) {
            double diff = contexts[i].msgs_per_sec - avg_rate;
            stddev += diff * diff;
        }
        stddev = sqrt(stddev / g_config.client_count);
        
        printf(" min %s | avg %s | max %s | stddev %s msgs\n",
               format_number((long)min_rate, rate_buf, sizeof(rate_buf)),
               format_number((long)avg_rate, buf, sizeof(buf)),
               format_number((long)max_rate, msg_buf, sizeof(msg_buf)),
               format_number((long)stddev, err_buf, sizeof(err_buf)));
    }
    
    if (total_errors > 0) {
        printf("\nErrors: %s\n", format_number(total_errors, err_buf, sizeof(err_buf)));
    }
    
    printf("\nCompleted in %.2fs\n", total_time);
    
    // Cleanup
    free(payload);
    free(threads);
    free(contexts);
    
    return 0;
}

int main(int argc, char *argv[]) {
    // Set locale for comma formatting
    setlocale(LC_NUMERIC, "");
    
    int ret = parse_args(argc, argv);
    if (ret != 0) {
        return ret < 0 ? 1 : 0;
    }
    
    // Initialize NATS library
    natsStatus s = nats_Open(-1);
    if (s != NATS_OK) {
        fprintf(stderr, "Error: Failed to initialize NATS library: %s\n", natsStatus_GetText(s));
        return 1;
    }
    
    if (strcmp(g_config.command, "pub") == 0) {
        ret = run_publish_benchmark();
    } else {
        fprintf(stderr, "Error: Unknown command '%s'\n", g_config.command);
        ret = 1;
    }
    
    // Close NATS library
    nats_Close();
    
    return ret;
}