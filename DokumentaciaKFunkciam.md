void com\_initialize(int nr\_proc, int \*rank)

{

}

com\_initialize() creates new processes by using fork() and adjusts their ranks by the pointer **\*rank.** Count of the processes, which have to be created, is specified in **nr\_proc**. Also allocates virtual memory by using mmap() for communication over shared memory.



void com\_finalize()

{

}

com\_finalize() ends all the processes. Cleans the memory.



void com\_recv(void \*\*message, size\_t \*size)

{

}

com\_recv() Reads by pointer **\*\*message** portion of memory of size specified by **\*size**.



void com\_send(int rank, void \*message, size\_t size)

{

}

com\_send() Writes to the part of memory of size specified by **size** message from pointer **\*message**, which process whose rank is **rank** have to read.



void com\_mcast(void \*message, size\_t size)

{

}

com\_mcast()  Writes to the part of memory of size specified by **size** message from pointer **\*message**, which all the processes, except of itself, have to read. 





