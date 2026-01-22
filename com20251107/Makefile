CC=gcc
CFLAGS=-c -O2 -ansi -pedantic -Wall -DLINUX $(INCL)
LFLAGS=
INCL=-I.
LIBS=
RM = "rm" -rf
AR = ar
ZIP= zip -9 -r

default:	libcom.a

libcom.a:	libcom.o
	$(RM) $@
	$(AR) r $@ com.o

libcom.o:	com.c com.h
	$(CC) $(CFLAGS) com.c

zip:		clean
	$(ZIP) com`date +'%m%d'`.zip *

clean:
	$(RM) libcom.a com.o
