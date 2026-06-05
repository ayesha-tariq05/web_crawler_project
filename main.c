#include <curl/curl.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "crawler.h"

int main(int argc, char *argv[])
{
    curl_global_init(CURL_GLOBAL_DEFAULT);

    if (argc < 4)
    {
        printf("Usage: %s <url_file> <total_threads> <chunk_size>\n", argv[0]);
        return 1;
    }

    char *input_file = argv[1];
    int T = atoi(argv[2]);
    int N = atoi(argv[3]);

    int D = T / 2;
    int P = T - D;

    init_queues();
    clock_gettime(CLOCK_MONOTONIC, &global_start);

    printf("Starting Multi-threaded Web Crawler\n");
    printf("[System] Threads: %d | Downloaders: %d | Parsers: %d\n", T, D, P);
    printf("[System] Chunk Size: %d words\n", N);

    pthread_t reader_thread;
    pthread_t downloader_threads[D];
    pthread_t parser_threads[P];

    DownloaderArgs d_args[D];
    ParserArgs p_args[P];

    //  ONLY GLOBAL TIMER (kept for full program runtime)

    // ---------------- Reader ----------------
    pthread_create(&reader_thread, NULL, url_reader, (void *)input_file);

    // ---------------- Downloaders ----------------
    for (int i = 0; i < D; i++)
    {
        d_args[i].thread_id = i + 1;
        d_args[i].chunk_size = N;

        pthread_create(&downloader_threads[i],
                       NULL,
                       downloader_stage,
                       &d_args[i]);
    }

    // ---------------- Parsers ----------------
    for (int i = 0; i < P; i++)
    {
        p_args[i].thread_id = i;

        pthread_create(&parser_threads[i],
                       NULL,
                       parser_stage,
                       &p_args[i]);
    }

    // ---------------- Join Reader ----------------
    pthread_join(reader_thread, NULL);

    // ---------------- Join Downloaders ----------------
    for (int i = 0; i < D; i++)
    {
        pthread_join(downloader_threads[i], NULL);
    }

    // ---------------- END SIGNALS ----------------
    for (int i = 0; i < P; i++)
    {
        Chunk end_chunk;
        strcpy(end_chunk.text, "END");
        end_chunk.chunk_id = -1;
        enqueue_chunk(end_chunk);
    }

    // ---------------- Join Parsers ----------------
    for (int i = 0; i < P; i++)
    {
        pthread_join(parser_threads[i], NULL);
    }

    // 🔵 END GLOBAL TIMER (ONLY TOTAL PROGRAM TIME)


    // ---------------- AGGREGATOR (ONLY OUTPUT OWNER) ----------------
    aggregate_results(parser_results, P);
    clock_gettime(CLOCK_MONOTONIC, &global_end);
    curl_global_cleanup();
    return 0;
}