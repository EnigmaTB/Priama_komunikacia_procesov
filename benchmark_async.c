#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include "com.h"

typedef enum {
    MSG_REQUEST = 1,
    MSG_RESPONSE = 2
} MsgType;

typedef struct {
    MsgType type;
    int requester_rank;
} AppMessage;

int iterations;
size_t msg_size;
int target_rank;

sem_t sem_req_slots, sem_req_items;
void *req_buffer = NULL;

sem_t sem_resp_slots, sem_resp_items;
void *resp_buffer = NULL;

pthread_mutex_t outbox_mutex = PTHREAD_MUTEX_INITIALIZER;

double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

void* receiver_thread(void* arg) {
    int total_to_receive = iterations * 2;

    for (int i = 0; i < total_to_receive; i++) {
        void *msg = NULL;
        size_t sz;
        com_recv(&msg, &sz);
        AppMessage *app_msg = (AppMessage*)msg;

        if (app_msg->type == MSG_REQUEST) {
            sem_wait(&sem_req_slots);
            req_buffer = msg;
            sem_post(&sem_req_items);
        } 
        else {
            sem_wait(&sem_resp_slots);
            resp_buffer = msg;
            sem_post(&sem_resp_items);
        }
    }
    return NULL;
}

void* handler_thread(void* arg) {
    for (int i = 0; i < iterations; i++) {
        sem_wait(&sem_req_items);
        AppMessage *req = (AppMessage*)req_buffer;
        int requester = req->requester_rank;
        com_free_msg(req);
        sem_post(&sem_req_slots);

        pthread_mutex_lock(&outbox_mutex);
        void *resp_ptr = com_alloc_msg(msg_size);
        AppMessage *resp = (AppMessage*)resp_ptr;
        resp->type = MSG_RESPONSE;
        com_send(requester, resp_ptr, msg_size);
        pthread_mutex_unlock(&outbox_mutex);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <nr_proc> <iterations> <msg_size>\n", argv[0]);
        return 1;
    }

    int nr_proc = atoi(argv[1]);
    iterations = atoi(argv[2]);
    msg_size = (size_t)atoi(argv[3]);

    if (msg_size < sizeof(AppMessage)) msg_size = sizeof(AppMessage);

    int rank;
    com_initialize(nr_proc, &rank);

    sem_init(&sem_req_slots, 0, 1);
    sem_init(&sem_req_items, 0, 0);
    sem_init(&sem_resp_slots, 0, 1);
    sem_init(&sem_resp_items, 0, 0);

    pthread_t rx_tid, handler_tid;
    pthread_create(&rx_tid, NULL, receiver_thread, NULL);
    pthread_create(&handler_tid, NULL, handler_thread, NULL);

    double start_time = 0;
    if (rank == 0) start_time = get_time();

    target_rank = (rank + 1) % nr_proc;

    for (int i = 0; i < iterations; i++) {
        pthread_mutex_lock(&outbox_mutex);
        void *req_ptr = com_alloc_msg(msg_size);
        AppMessage *req = (AppMessage*)req_ptr;
        req->type = MSG_REQUEST;
        req->requester_rank = rank;
        com_send(target_rank, req_ptr, msg_size);
        pthread_mutex_unlock(&outbox_mutex);

        sem_wait(&sem_resp_items);
        AppMessage *resp = (AppMessage*)resp_buffer;
        com_free_msg(resp);
        sem_post(&sem_resp_slots);
    }

    pthread_join(rx_tid, NULL);
    pthread_join(handler_tid, NULL);

    if (rank == 0) {
        double total_time = get_time() - start_time;
        long total_messages = (long)iterations * nr_proc * 2;
        double msgs_per_sec = total_messages / total_time;
        double mb_per_sec = (total_messages * msg_size) / (1024.0 * 1024.0 * total_time);
        double latency_us = (total_time / total_messages) * 1000000.0;
        printf("%d %d %zu %f %.0f %.2f %.2f\n", 
               nr_proc, iterations, msg_size, total_time, msgs_per_sec, mb_per_sec, latency_us);
    }

    com_finalize();
    return 0;
}