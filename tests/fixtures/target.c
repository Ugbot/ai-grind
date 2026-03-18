#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* A small program with syscalls and allocations for DTrace + LLDB testing. */

static char *alloc_greeting(const char *name) {
    size_t len = strlen("Hello, ") + strlen(name) + 2;
    char *buf = malloc(len);
    if (!buf) return NULL;
    snprintf(buf, len, "Hello, %s!", name);
    return buf;
}

static int sum_range(int lo, int hi) {
    int total = 0;
    for (int i = lo; i <= hi; i++) {
        total += i;
    }
    return total;
}

static void do_file_io(void) {
    FILE *f = fopen("/tmp/devtools_mcp_test.txt", "w");
    if (!f) return;
    for (int i = 0; i < 10; i++) {
        fprintf(f, "line %d: the quick brown fox\n", i);
    }
    fclose(f);

    f = fopen("/tmp/devtools_mcp_test.txt", "r");
    if (!f) return;
    char line[128];
    while (fgets(line, sizeof(line), f)) {
        /* just consume */
    }
    fclose(f);
    unlink("/tmp/devtools_mcp_test.txt");
}

int main(int argc, char **argv) {
    const char *names[] = {"Alice", "Bob", "Charlie", "Diana", "Eve"};
    int n = sizeof(names) / sizeof(names[0]);

    /* Heap allocations */
    char *greetings[5];
    for (int i = 0; i < n; i++) {
        greetings[i] = alloc_greeting(names[i]);
        if (greetings[i]) {
            printf("%s\n", greetings[i]);
        }
    }

    /* Computation */
    int result = sum_range(1, 1000000);
    printf("sum(1..1000000) = %d\n", result);

    /* File I/O — generates open/read/write/close syscalls */
    do_file_io();

    /* Cleanup */
    for (int i = 0; i < n; i++) {
        free(greetings[i]);
    }

    printf("done.\n");
    return 0;
}
