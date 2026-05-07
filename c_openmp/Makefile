# File description:
# Simple build rules for the focused OpenMP comparison kernel.

CC ?= gcc
CFLAGS ?= -O3 -fopenmp -std=c11 -Wall -Wextra
LDFLAGS ?= -lm

all: spike_kernel

spike_kernel: spike_kernel.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f spike_kernel
