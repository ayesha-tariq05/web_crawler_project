# Multi-Threaded Web Crawler & Text Analytics System

## Project Overview

A multi-threaded web crawler developed in C using POSIX Threads and libcurl. The system downloads text content from multiple URLs concurrently, processes the data in parallel using parser threads, and generates statistical insights such as total word count, average word length, longest word, word-length distribution, and performance metrics.

## Key Features

* Multi-threaded architecture with Downloader, Parser, and Aggregator stages.
* Producer-Consumer model using synchronized queues.
* Parallel downloading and text processing.
* Thread-safe communication using mutexes and condition variables.
* Performance measurement for each pipeline stage.
* Statistical text analysis and reporting.

## Technical Challenges Faced

### 1. Thread Synchronization

Managing multiple downloader and parser threads without race conditions was one of the biggest challenges. Proper use of mutexes and condition variables was required to ensure safe access to shared queues.

### 2. Producer-Consumer Coordination

Designing URL and Chunk queues that correctly handled concurrent enqueue and dequeue operations while preventing deadlocks and data corruption.

### 3. Thread Termination

Implementing a clean shutdown mechanism using END markers. Ensuring that all parser threads terminated correctly without leaving any thread blocked indefinitely.

### 4. Debugging Concurrent Execution

Since multiple threads execute simultaneously, tracking program flow and identifying synchronization issues required extensive debugging and logging.

### 5. Text Parsing Accuracy

Handling punctuation, special characters, numbers, and tokenization correctly while calculating word statistics and distributions.

## Major Learnings

* Practical implementation of POSIX Threads (pthreads).
* Synchronization using mutexes and condition variables.
* Producer-Consumer design pattern.
* Multi-threaded pipeline architecture.
* Performance analysis and throughput measurement.
* Debugging concurrent systems.
* Real-world usage of libcurl for web content retrieval.

## What Makes This Project Stand Out

* Combines networking, concurrency, synchronization, and text analytics in a single system.
* Implements a complete multi-stage processing pipeline.
* Demonstrates real-world parallel programming concepts.
* Includes both functional analytics and performance evaluation.
* Scalable design that can handle multiple download and parser threads simultaneously.

## Technologies Used

* C Programming
* POSIX Threads (pthreads)
* libcurl
* Mutexes
* Condition Variables
* Linux Environment
