#include <time.h>
#include "crawler.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <curl/curl.h>
#include <ctype.h>
#include <pthread.h>
#define NUM_PARSERS 15
#define NUM_DOWNLOADERS 15
pthread_mutex_t time_lock = PTHREAD_MUTEX_INITIALIZER;
/* ================= GLOBAL STATE ================= */

URLQueue url_queue;
ChunkQueue chunk_queue;
ParserResult parser_results[50];

struct timespec global_start, global_end;

int global_chunk_id = 0;

/* performance */
double download_time = 0;
double parse_time = 0;
double aggregator_time = 0;

/* locks */
pthread_mutex_t chunk_id_lock = PTHREAD_MUTEX_INITIALIZER;

/* ================= TIME ================= */

double time_diff(struct timespec a, struct timespec b)
{
    return (b.tv_sec - a.tv_sec) +
           (b.tv_nsec - a.tv_nsec) / 1e9;
}

/* ================= INIT ================= */

void init_queues()
{
    memset(parser_results, 0, sizeof(parser_results));

    url_queue.head = url_queue.tail = url_queue.count = 0;
    chunk_queue.head = chunk_queue.tail = chunk_queue.count = 0;

    pthread_mutex_init(&url_queue.lock, NULL);
    pthread_mutex_init(&chunk_queue.lock, NULL);

    pthread_cond_init(&url_queue.not_full, NULL);
    pthread_cond_init(&url_queue.not_empty, NULL);

    pthread_cond_init(&chunk_queue.not_full, NULL);
    pthread_cond_init(&chunk_queue.not_empty, NULL);
}

/* ================= CURL ================= */

typedef struct
{
    char *data;
    size_t size;
} MemoryBuffer;

size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t real = size * nmemb;
    MemoryBuffer *mem = (MemoryBuffer *)userp;

    char *ptr = realloc(mem->data, mem->size + real + 1);
    if (!ptr)
        return 0;

    mem->data = ptr;
    memcpy(mem->data + mem->size, contents, real);

    mem->size += real;
    mem->data[mem->size] = '\0';

    return real;
}

/* ================= QUEUE OPS ================= */

void enqueue_url(URLQueue *q, char *url)
{
    pthread_mutex_lock(&q->lock);

    while (q->count == URL_QUEUE_SIZE)
        pthread_cond_wait(&q->not_full, &q->lock);

    strcpy(q->urls[q->tail], url);
    q->tail = (q->tail + 1) % URL_QUEUE_SIZE;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

char *dequeue_url(URLQueue *q)
{
    pthread_mutex_lock(&q->lock);

    while (q->count == 0)
        pthread_cond_wait(&q->not_empty, &q->lock);

    char *url = malloc(256);
    strcpy(url, q->urls[q->head]);

    q->head = (q->head + 1) % URL_QUEUE_SIZE;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);

    return url;
}

void enqueue_chunk(Chunk c)
{
    pthread_mutex_lock(&chunk_queue.lock);

    while (chunk_queue.count == CHUNK_QUEUE_SIZE)
        pthread_cond_wait(&chunk_queue.not_full, &chunk_queue.lock);

    chunk_queue.chunks[chunk_queue.tail] = c;
    chunk_queue.tail = (chunk_queue.tail + 1) % CHUNK_QUEUE_SIZE;
    chunk_queue.count++;

    pthread_cond_signal(&chunk_queue.not_empty);
    pthread_mutex_unlock(&chunk_queue.lock);
}

Chunk dequeue_chunk()
{
    pthread_mutex_lock(&chunk_queue.lock);

    while (chunk_queue.count == 0)
        pthread_cond_wait(&chunk_queue.not_empty, &chunk_queue.lock);

    Chunk c = chunk_queue.chunks[chunk_queue.head];

    chunk_queue.head = (chunk_queue.head + 1) % CHUNK_QUEUE_SIZE;
    chunk_queue.count--;

    pthread_cond_signal(&chunk_queue.not_full);
    pthread_mutex_unlock(&chunk_queue.lock);

    return c;
}

/* ================= URL READER ================= */

void *url_reader(void *arg)
{
    char *file = (char *)arg;
    FILE *fp = fopen(file, "r");

    if (!fp)
    {
        perror("URL file error");
        return NULL;
    }

    char line[256];

    while (fgets(line, sizeof(line), fp))
    {
        line[strcspn(line, "\r\n")] = 0;
        if (strlen(line) > 0)
        {
            printf("[Reader] URL: %s\n", line);
            enqueue_url(&url_queue, line);
        }
    }

    fclose(fp);

    /* ONE END for URL stream */
    enqueue_url(&url_queue, "END");

    return NULL;
}

/* ================= DOWNLOADER ================= */

void *downloader_stage(void *arg)
{
    DownloaderArgs *args = (DownloaderArgs *)arg;

    while (1)
    {
        char *url = dequeue_url(&url_queue);

        if (strcmp(url, "END") == 0)
        {
            enqueue_url(&url_queue, "END"); // propagate
            free(url);
            break;
        }

        printf("[Downloader] URL: %s\n", url);

        MemoryBuffer buf = {malloc(1), 0};

        struct timespec s, e;
        clock_gettime(CLOCK_MONOTONIC, &s);

        CURL *curl = curl_easy_init();

        if (curl)
        {
            curl_easy_setopt(curl, CURLOPT_URL, url);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

            curl_easy_perform(curl);

            clock_gettime(CLOCK_MONOTONIC, &e);
            download_time += time_diff(s, e);

            char *copy = strdup(buf.data);

            char *word = strtok(copy,
                                " \n\t\r.,!?;:\"()[]{}<>-/=_0123456789—");

            char chunk[50000] = "";
            
            int count = 0;

            while (word)
            {
                strcat(chunk, word);
                strcat(chunk, " ");
                count++;

                if (count == args->chunk_size)
                {
                    Chunk c;
                    strcpy(c.text, chunk);

                    pthread_mutex_lock(&chunk_id_lock);
                    c.chunk_id = global_chunk_id++;
                    pthread_mutex_unlock(&chunk_id_lock);
printf("[Downloader Chunk] %d => %s\n", c.chunk_id, c.text);
                    enqueue_chunk(c);

                    chunk[0] = '\0';
                    count = 0;
                }

                word = strtok(NULL,
                              " \n\t\r.,!?;:\"()[]{}<>-/=_0123456789—");
            }

            if (count > 0)
            {
                Chunk c;
                strcpy(c.text, chunk);

                pthread_mutex_lock(&chunk_id_lock);
                c.chunk_id = global_chunk_id++;
                pthread_mutex_unlock(&chunk_id_lock);

                enqueue_chunk(c);
            }

            free(copy);
            curl_easy_cleanup(curl);
        }

        free(buf.data);
        free(url);
    }

    return NULL;
}

/* ================= PARSER ================= */

void *parser_stage(void *arg)
{
    ParserArgs *args = (ParserArgs *)arg;
    int id = args->thread_id;

    ParserResult *r = &parser_results[id];

    memset(r, 0, sizeof(ParserResult));

    struct timespec start, end;

    while (1)
    {
        Chunk c = dequeue_chunk();

        // END chunk
        if (strcmp(c.text, "END") == 0)
        {
            break;
        }

        clock_gettime(CLOCK_MONOTONIC, &start);

        char copy[50000];
        strcpy(copy, c.text);

        char *word = strtok(copy,
            " \n\t\r.,!?;:\"()[]{}<>-/=_0123456789");

        while (word != NULL)
        {
            int len = strlen(word);

            // Skip empty words
            if (len <= 0)
            {
                word = strtok(NULL,
                    " \n\t\r.,!?;:\"()[]{}<>-/=_0123456789");
                continue;
            }

            // Count only alphabetic words
            int is_valid = 1;

            for (int i = 0; i < len; i++)
            {
                if (!isalpha((unsigned char)word[i]))
                {
                    is_valid = 0;
                    break;
                }
            }

            if (is_valid)
            {
                // Total words
                r->total_words++;

                // Total word length
                r->total_word_length += len;

                // Longest word
                if (len > r->longest_length)
                {
                    r->longest_length = len;
                    strcpy(r->longest_word, word);
                }

                // Word length distribution
                if (len >= 20)
                {
                    r->word_length_distribution[19]++;
                }
                else
                {
                    r->word_length_distribution[len - 1]++;
                }
            }

            word = strtok(NULL,
                " \n\t\r.,!?;:\"()[]{}<>-/=_0123456789");
        }

        clock_gettime(CLOCK_MONOTONIC, &end);

        double elapsed = time_diff(start, end);

        pthread_mutex_lock(&time_lock);
        parse_time += elapsed;
        pthread_mutex_unlock(&time_lock);
    }

    return NULL;
}
/* ================= AGGREGATOR (ONLY FINAL OUTPUT) ================= */

void aggregate_results(ParserResult parser_results[], int P)
{
    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);

    printf("\n========== AGGREGATOR STARTED ==========\n");

    ParserResult global;

    memset(&global, 0, sizeof(ParserResult));

    // ================= MERGE ALL PARSER RESULTS =================
    for (int i = 0; i < P; i++)
    {
        // Total words
        global.total_words += parser_results[i].total_words;

        // Total word length
        global.total_word_length +=
            parser_results[i].total_word_length;

        // Longest word
        if (parser_results[i].longest_length >
            global.longest_length)
        {
            global.longest_length =
                parser_results[i].longest_length;

            strcpy(global.longest_word,
                   parser_results[i].longest_word);
        }

        // ================= MERGE DISTRIBUTION =================
        for (int j = 0; j < 20; j++)
        {
            global.word_length_distribution[j] +=
                parser_results[i].word_length_distribution[j];
        }
    }

    // ================= AVERAGE =================
    double avg = 0.0;

    if (global.total_words > 0)
    {
        avg = (double)global.total_word_length /
              global.total_words;
    }

    clock_gettime(CLOCK_MONOTONIC, &end);

    aggregator_time = time_diff(start, end);

    // ================= PRINT RESULTS =================
    printf("\n========== FINAL RESULTS ==========\n");

    printf("Global Longest Word: %s\n",
           global.longest_word);

    printf("Global Longest Length: %d\n",
           global.longest_length);

    printf("\nTotal Parsed Words: %ld\n",
           global.total_words);

    printf("Average Word Length: %.2f\n",
           avg);

    // ================= WORD DISTRIBUTION =================
    printf("\n========== WORD LENGTH DISTRIBUTION ==========\n");

    for (int i = 0; i < 19; i++)
    {
        printf("Length %2d : %d\n",
               i + 1,
               global.word_length_distribution[i]);
    }

    printf("Length 20+ : %d\n",
           global.word_length_distribution[19]);

    // ================= PERFORMANCE =================
    printf("\n========== PERFORMANCE ==========\n");

    printf("Downloader Time: %.4f\n",
           download_time);

    printf("Parser Time: %.4f\n",
           parse_time);

    printf("Aggregator Time: %.4f\n",
           aggregator_time);

    double total_execution =
        download_time +
        parse_time +
        aggregator_time;

    printf("Total Execution Time: %.4f\n",
           total_execution);

    if (total_execution > 0)
    {
        printf("Throughput: %.2f words/sec\n",
               global.total_words / total_execution);
    }

    // ================= SAVE TO FILE =================
    FILE *fp = fopen("stats.txt", "w");

    if (fp)
    {
        fprintf(fp,
                "Global Longest Word: %s\n",
                global.longest_word);

        fprintf(fp,
                "Global Longest Length: %d\n",
                global.longest_length);

        fprintf(fp,
                "Total Parsed Words: %ld\n",
                global.total_words);

        fprintf(fp,
                "Average Word Length: %.2f\n",
                avg);

        fprintf(fp,
                "\nWORD LENGTH DISTRIBUTION\n");

        for (int i = 0; i < 19; i++)
        {
            fprintf(fp,
                    "Length %2d : %d\n",
                    i + 1,
                    global.word_length_distribution[i]);
        }

        fprintf(fp,
                "Length 20+ : %d\n",
                global.word_length_distribution[19]);

        fclose(fp);

        printf("\n[Aggregator] stats.txt updated successfully.\n");
    }
    else
    {
        printf("\n[Aggregator] Failed to write stats.txt\n");
    }

    printf("\n========== AGGREGATOR FINISHED ==========\n");
}