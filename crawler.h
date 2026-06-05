#ifndef CRAWLER_H
#define CRAWLER_H

#include <pthread.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ================= CONSTANTS ================= */
#define URL_QUEUE_SIZE 5
#define CHUNK_QUEUE_SIZE 20
extern struct timespec global_start, global_end;
/* ================= STRUCT DEFINITIONS ================= */

// --- Parser Result (MUST be first) ---
typedef struct
{
    char longest_word[256];
    int longest_length;

    long total_word_length;
    long total_words;

    int word_length_distribution[20];

} ParserResult;

// --- URL Queue ---
typedef struct {
    char urls[URL_QUEUE_SIZE][256];
    int head, tail, count;

    pthread_mutex_t lock;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} URLQueue;

// --- Chunk ---
typedef struct
{
    char text[50000];
    int chunk_id;
} Chunk;

// --- Chunk Queue ---
typedef struct
{
    Chunk chunks[CHUNK_QUEUE_SIZE];
    int head, tail, count;

    pthread_mutex_t lock;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} ChunkQueue;

// --- Args ---
typedef struct
{
    int thread_id;
    int chunk_size;
} DownloaderArgs;

typedef struct
{
    int thread_id;
} ParserArgs;

/* ================= GLOBALS ================= */

extern URLQueue url_queue;
extern ChunkQueue chunk_queue;
extern ParserResult parser_results[50];

extern double download_time;
extern double parse_time;
extern double aggregator_time;
extern long total_parsed_words;
double time_diff(struct timespec start, struct timespec end);
/* ================= FUNCTION PROTOTYPES ================= */

void init_queues();
void enqueue_url(URLQueue *q, char *url);
char* dequeue_url(URLQueue *q);

void enqueue_chunk(Chunk chunk);
Chunk dequeue_chunk();

void* url_reader(void* arg);
void* downloader_stage(void* arg);
void* parser_stage(void* arg);

void aggregate_results(ParserResult parser_results[], int P);

#endif