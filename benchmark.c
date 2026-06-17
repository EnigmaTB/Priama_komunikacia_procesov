#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <string.h>
#include "com.h"

double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <nr_proc> <iterations> <msg_size>\n", argv[0]);
        return 1;
    }

    int nr_proc = atoi(argv[1]);
    int iterations = atoi(argv[2]);
    size_t msg_size = (size_t)atoi(argv[3]);

    if (nr_proc < 2) {
        fprintf(stderr, "Error: nr_proc must be at least 2.\n");
        return 1;
    }

    int rank;
    com_initialize(nr_proc, &rank);

    if (rank == 0) {
        double start_time = get_time();

        for (int i = 0; i < iterations; i++) {
            void *send_buf = com_alloc_msg(msg_size);
           
            com_mcast(send_buf, msg_size);

            for (int p = 1; p < nr_proc; p++) {
                void *recv_buf = NULL;
                size_t recv_size;
                
                com_recv(&recv_buf, &recv_size);
                com_free_msg(recv_buf);
            }
        }

        double end_time = get_time();
        double total_time = end_time - start_time;
        long total_messages = (long)iterations * nr_proc;
        
        double msgs_per_sec = total_messages / total_time;
        double mb_per_sec = (total_messages * msg_size) / (1024.0 * 1024.0 * total_time);
        double latency_us = (total_time / total_messages) * 1000000.0;

        printf("%d %d %zu %f %.0f %.2f %.2f\n", 
               nr_proc, iterations, msg_size, total_time, msgs_per_sec, mb_per_sec, latency_us);

    } else {
        
        for (int i = 0; i < iterations; i++) {
            void *recv_buf = NULL;
            size_t recv_size;
            
            com_recv(&recv_buf, &recv_size);
            com_free_msg(recv_buf);

            void *send_buf = com_alloc_msg(msg_size);
            com_send(0, send_buf, msg_size);
        }
    }

    com_finalize();
    return 0;
}