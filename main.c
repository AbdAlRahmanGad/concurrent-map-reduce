#include <stdio.h>
#include <pthread.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            // debug print
            printf("Emitting [%s, 1]\n", token);
            MR_Emit(token, "1");
            printf("%lu\n", MR_DefaultHashPartition(token, 10));

        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
    int count = 0;
    char *value;
    while ((value = get_next(key, partition_number)) != NULL)
        count++;
    printf("%s %d\n", key, count);
}

int main(int argc, char *argv[]) {
    /// for testing purposes
//    unsigned long x = MR_DefaultHashPartition("helloklfj;fjdasj", 10);
//    unsigned long xd = MR_DefaultHashPartition("dddd", 10);
//    unsigned long xe = MR_DefaultHashPartition("hello", 10);
//    printf("%lu\n", x);
//    printf("%lu\n", xd);
//    printf("%lu\n", xe);
//    Map(argv[1]);
//    MR_Run(argc, argv, Map, 1, Reduce, 10, MR_DefaultHashPartition);
//    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
//    MR_Run(argc, argv, Map, 5, Reduce, 10, MR_DefaultHashPartition);
    MR_Run(argc, argv, Map, 20, Reduce, 10, MR_DefaultHashPartition);

}