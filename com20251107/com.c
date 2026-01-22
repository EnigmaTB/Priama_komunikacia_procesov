/****************************************************************************
*                com.c
*****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>

/*****************************************************************************
* Local preprocessor defines
******************************************************************************/

/*****************************************************************************
* Local typedefs
******************************************************************************/

/*****************************************************************************
* Local variables
******************************************************************************/

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

void com_initialize(int nr_proc, int *rank){
    for (int i = 1; i < nr_proc; ++i) {
        pid_t p = fork();
        
        if (p < 0) {
            
            perror("fork failed");
            exit(1);
        }

        if (p == 0) {
            *rank = i;  
            return;    
        }
        *rank = 0;
    }
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

void com_finalize(){
    while (waitpid(-1, NULL, 0) > 0) {}
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

void com_recv(void **message, size_t *size)
{
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

void com_send(int rank, void *message, size_t size)
{
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

void com_mcast(void *message, size_t size)
{
}
