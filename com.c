/****************************************************************************
*                com.c
******************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <fcntl.h>
#include <semaphore.h>
#include <pthread.h>

/*****************************************************************************
* Local preprocessor defines
******************************************************************************/

/*****************************************************************************
* Local typedefs
******************************************************************************/
typedef struct {
    int target_rank;            
    size_t size;                
    int readers_left;           
    int *has_read;              
} MessageDesc;

typedef struct {
    sem_t mutex;
    int nr_proc;
    sem_t *sem_recv;            
    sem_t *sem_send;            
    MessageDesc *messages;      
} ControlBlock;

typedef struct {
    void *ptr;
    size_t size;
    int active;
} RecvTrack;
/*****************************************************************************
* Local variables
******************************************************************************/
ControlBlock *ctrl = NULL;  
int my_rank = -1;           
int last_read_rank = -1;

int my_shm_fd = -1;             
void *my_shm_ptr = NULL;        
size_t my_shm_capacity = 0;     
char my_shm_name[64];

RecvTrack *recv_track = NULL;
pthread_mutex_t track_mutex = PTHREAD_MUTEX_INITIALIZER;
/*****************************************************************************
* Implementation of functions
******************************************************************************/


/*****************************************************************************
*
* FUNCTION
*
*   com_initialize
*
* INPUT
*
*   int nr_proc - number of processes
*
* OUTPUT
*
*   int *rank - pointer to the rank of a process (0... N-1)
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Forks processes given by input, prepares communication over shared memory
*
******************************************************************************/

void com_initialize(int nr_proc, int *rank) {
    for (int i = 0; i < nr_proc; i++) {
        char stale_name[64];
        sprintf(stale_name, "/shm_data_%d", i);
        shm_unlink(stale_name); 
    }

    size_t size_cb      = sizeof(ControlBlock);
    size_t size_recv    = nr_proc * sizeof(sem_t);
    size_t size_send    = nr_proc * sizeof(sem_t);
    size_t size_msgs    = nr_proc * sizeof(MessageDesc);
    size_t size_flags   = nr_proc * nr_proc * sizeof(int); 

    size_t total_memory = size_cb + size_recv + size_send + size_msgs + size_flags;

    ctrl = mmap(NULL, total_memory, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (ctrl == MAP_FAILED) {
        perror("mmap failed for ControlBlock");
        exit(1);
    }

    ctrl->sem_recv = (sem_t*)((char*)ctrl + size_cb);
    ctrl->sem_send = (sem_t*)((char*)ctrl->sem_recv + size_recv);
    ctrl->messages = (MessageDesc*)((char*)ctrl->sem_send + size_send);
    int *flags_base = (int*)((char*)ctrl->messages + size_msgs);

    ctrl->nr_proc = nr_proc;
    sem_init(&ctrl->mutex, 1, 1);

    for (int i = 0; i < nr_proc; i++) {
        sem_init(&ctrl->sem_recv[i], 1, 0);
        sem_init(&ctrl->sem_send[i], 1, 1);
        ctrl->messages[i].target_rank = -2; 
        ctrl->messages[i].has_read = flags_base + (i * nr_proc); 
    }

    for (int i = 1; i < nr_proc; ++i) {
        pid_t p = fork();
        if (p < 0) {
            perror("fork failed");
            exit(1);
        }
        if (p == 0) {
            *rank = i;
            my_rank = i;
            sprintf(my_shm_name, "/shm_data_%d", my_rank);
            recv_track = calloc(nr_proc, sizeof(RecvTrack));
            return; 
        }
    }
    *rank = 0;
    my_rank = 0;
    sprintf(my_shm_name, "/shm_data_%d", my_rank);
    recv_track = calloc(nr_proc, sizeof(RecvTrack));
}

/*****************************************************************************
*
* FUNCTION
*
*   com_finalize
*
* INPUT
*
* OUTPUT
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Cleanly terminates processes
*
******************************************************************************/

void com_finalize() {
    if (my_rank == 0) {
        while (waitpid(-1, NULL, 0) > 0) {}

        sem_destroy(&ctrl->mutex);
        for (int i = 0; i < ctrl->nr_proc; i++) {
            sem_destroy(&ctrl->sem_recv[i]);
            sem_destroy(&ctrl->sem_send[i]);
        }
        
        size_t size_cb      = sizeof(ControlBlock);
        size_t size_recv    = ctrl->nr_proc * sizeof(sem_t);
        size_t size_send    = ctrl->nr_proc * sizeof(sem_t);
        size_t size_msgs    = ctrl->nr_proc * sizeof(MessageDesc);
        size_t size_flags   = ctrl->nr_proc * ctrl->nr_proc * sizeof(int);
        
        munmap(ctrl, size_cb + size_recv + size_send + size_msgs + size_flags);
        
        if (my_shm_ptr != NULL) {
            munmap(my_shm_ptr, my_shm_capacity);
            shm_unlink(my_shm_name);
        }
    } else {
        while (sem_wait(&ctrl->sem_send[my_rank]) == -1);

        if (my_shm_ptr != NULL) {
            munmap(my_shm_ptr, my_shm_capacity);
            shm_unlink(my_shm_name);
        }
        exit(0);
    }
    
    if (recv_track) free(recv_track);
}

/*****************************************************************************
*
* FUNCTION
*
* com_alloc_msg
*
* INPUT
*
* size_t size - the requested size of the message buffer in bytes
*
* OUTPUT
*
* None
*
* RETURNS
*
* void* - pointer to the allocated shared memory buffer
*
* DESCRIPTION
*
* Allocates or reallocates a shared memory buffer for sending a message.
*
******************************************************************************/

void* com_alloc_msg(size_t size) {
    sem_wait(&ctrl->sem_send[my_rank]);
    
    if (my_shm_fd == -1) {
        my_shm_fd = shm_open(my_shm_name, O_CREAT | O_RDWR | O_TRUNC, 0666);
        if (my_shm_fd == -1) {
            perror("shm_open failed"); exit(1);
        }

        
        if (ftruncate(my_shm_fd, size) == -1) {
            perror("ftruncate failed (out of memory in /dev/shm)"); exit(1);
        }

        my_shm_ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, my_shm_fd, 0);
        if (my_shm_ptr == MAP_FAILED) {
            perror("mmap failed"); exit(1);
        }

        my_shm_capacity = size;
    } 
    else if (size > my_shm_capacity) {
        munmap(my_shm_ptr, my_shm_capacity); 

        if (ftruncate(my_shm_fd, size) == -1) {
            perror("ftruncate failed (out of memory in /dev/shm)"); exit(1);
        }

        my_shm_ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, my_shm_fd, 0);
        if (my_shm_ptr == MAP_FAILED) {
            perror("mmap failed"); exit(1);
        }

        my_shm_capacity = size;
    }
    
    return my_shm_ptr; 
}

/*****************************************************************************
*
* FUNCTION
*
* com_free_msg
*
* INPUT
*
* void *msg - pointer to the previously received shared memory message
*
* OUTPUT
*
* None
*
* RETURNS
*
* void
*
* DESCRIPTION
*
* Marks a received message as consumed and unmaps it from the address space.
*
******************************************************************************/

void com_free_msg(void *msg) {
    if (msg == NULL) return;

    int sender = -1;
    size_t size = 0;

    pthread_mutex_lock(&track_mutex);
    for (int i = 0; i < ctrl->nr_proc; i++) {
        if (recv_track[i].active && recv_track[i].ptr == msg) {
            sender = i;
            size = recv_track[i].size;
            recv_track[i].active = 0;
            break;
        }
    }
    pthread_mutex_unlock(&track_mutex);

    if (sender == -1) return;

    int should_wake_sender = 0;
    while (sem_wait(&ctrl->mutex) == -1);
    
    if (ctrl->messages[sender].target_rank == -1) {
        ctrl->messages[sender].readers_left--;
        if (ctrl->messages[sender].readers_left == 0) {
            ctrl->messages[sender].target_rank = -2;
            should_wake_sender = 1;
        }
    } else {
        ctrl->messages[sender].target_rank = -2;
        should_wake_sender = 1;
    }
    sem_post(&ctrl->mutex);

    munmap(msg, size);

    if (should_wake_sender) {
        sem_post(&ctrl->sem_send[sender]);
    }
}

/*****************************************************************************
*
* FUNCTION
*
*   com_recv
*
* INPUT
*
* OUTPUT
*   void **message - pointer to the received message
*   size_t *size - pointer to the size of the received message
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Receives a message
*
******************************************************************************/

void com_recv(void **message, size_t *size) {
    int sender = -1;
    int is_mcast = 0;

    while (1) {
        while (sem_wait(&ctrl->sem_recv[my_rank]) == -1);
        while (sem_wait(&ctrl->mutex) == -1);
        
        for (int step = 1; step <= ctrl->nr_proc; step++) {
            int i = (last_read_rank + step) % ctrl->nr_proc;
            if (ctrl->messages[i].target_rank == my_rank || 
               (ctrl->messages[i].target_rank == -1 && ctrl->messages[i].has_read[my_rank] == 0)) {
                sender = i;
                is_mcast = (ctrl->messages[i].target_rank == -1);
                last_read_rank = i; 
                break;
            }
        }
        
        if (sender != -1) break;
        sem_post(&ctrl->mutex);
    }
    
    *size = ctrl->messages[sender].size;

    if (is_mcast) {
        ctrl->messages[sender].has_read[my_rank] = 1;
    } else {
        ctrl->messages[sender].target_rank = -3; 
    }
    sem_post(&ctrl->mutex);
    char sender_shm_name[64];
    sprintf(sender_shm_name, "/shm_data_%d", sender);

    int fd = shm_open(sender_shm_name, O_RDONLY, 0666);
    if (fd == -1) { perror("shm_open failed"); exit(1); }

    void *ptr = mmap(NULL, *size, PROT_READ, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap failed"); exit(1); }
    close(fd);

    pthread_mutex_lock(&track_mutex);
    recv_track[sender].ptr = ptr;
    recv_track[sender].size = *size;
    recv_track[sender].active = 1;
    pthread_mutex_unlock(&track_mutex);

    *message = ptr;
}

/*****************************************************************************
*
* FUNCTION
*
*   com_send
*
* INPUT
*
*   int rank - rank of the recipient
*   void *message - the message being sent
*   size_t size - size of the message being sent
*
* OUTPUT
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Sends a message
*
******************************************************************************/

void com_send(int target_rank, void *message, size_t size) {
    while (sem_wait(&ctrl->mutex) == -1);
    ctrl->messages[my_rank].target_rank = target_rank;
    ctrl->messages[my_rank].size = size;
    sem_post(&ctrl->mutex);

    sem_post(&ctrl->sem_recv[target_rank]);
}

/*****************************************************************************
*
* FUNCTION
*
*   com_mcast
*
* INPUT
*
* OUTPUT
*   void *message - the message being sent
*   size_t size - size of the message being sent
*
* RETURNS
*
*   void
*
* DESCRIPTION
*
*   Sends (multicasts) a message to all processes except to itself
*
******************************************************************************/

void com_mcast(void *message, size_t size) {
    while (sem_wait(&ctrl->mutex) == -1);
    ctrl->messages[my_rank].target_rank = -1; 
    ctrl->messages[my_rank].size = size;
    
    ctrl->messages[my_rank].readers_left = ctrl->nr_proc - 1; 
    for(int k = 0; k < ctrl->nr_proc; k++) {
        ctrl->messages[my_rank].has_read[k] = 0;
    }
    ctrl->messages[my_rank].has_read[my_rank] = 1; 
    sem_post(&ctrl->mutex);

    for (int i = 0; i < ctrl->nr_proc; i++) {
        if (i != my_rank) {
            sem_post(&ctrl->sem_recv[i]);
        }
    }
}