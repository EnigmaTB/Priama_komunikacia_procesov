#include <stdio.h>      
#include <stdlib.h>
#include <unistd.h>     
#include "com.h"

int faktorial(int i) {
    if (i < 2) {
        return i;
    } else {
        return i * faktorial(i-1); 
    }
}

int main(void){
    int rank;
    int nr_proc = 5;

    com_initialize(nr_proc, &rank);

    if (rank == 0) {
        printf("MASTER (Rank 0): I am supervising everyone. PID=%d\n", getpid());
    }

    if (rank == 1) {
        printf("CHILD (Rank 1). I am the nearest process to MASTER. PID=%d\n", getpid());
    }
    if (rank == 2) {
        printf("CHILD (Rank 2). Sum of 2 and 2 is %d. PID=%d\n", 2 + 2, getpid());
    }
    if (rank > 2){
        printf("CHILD (Rank %d). Faktorial of my rank is %d. PID=%d\n", rank, faktorial(rank),
         getpid());
    }

    com_finalize();

    return 0;
}
