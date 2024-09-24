//
// Created by abdobngad on 9/24/24.
//
#include "mapreduce.h"
#include <pthread.h>
#include <malloc.h>

typedef struct pair{
    char *key;
    char *value;
    struct pair *next;
} pair;

Partitioner partition_algo;
int num_partitions;
pair *pairs;

void MR_Emit(char *key, char *value){

    unsigned long partition = partition_algo(key, num_partitions);
    if(pairs[partition].key == NULL){
        pairs[partition].key = key;
        pairs[partition].value = value;
        pairs[partition].next = NULL;
        return;
    }else {
        pair *p = malloc(sizeof(pair));
        p->key = key;
        p->value = value;
        p->next = NULL;
        pairs[partition].next = p;
    }
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

typedef struct {
    int partition_number;
    Reducer reduce;
} ReduceArgs;

void *ReduceThread(void *args){
    /// TODO we will need locks here or in Getter or both
    /// TODO finish this function

//    ReduceArgs *reduceArgs = (ReduceArgs *)args;
//    Reducer reduceFunction = reduceArgs->reduce;

//    reduceFunction(,, reduceArgs->partition_number);
    return NULL;
}



/**
 *
 * @param argc number of files
 * @param argv array of file names
 * @param map map function
 * @param num_mappers number of mappers
 * @param reduce reduce function
 * @param num_reducers number of reducers
 * @param partition partition function
 */
void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition){

    pthread_t reducers[num_reducers];
    pthread_t mappers[num_mappers];
    num_partitions = num_reducers;
    partition_algo = partition;

    pairs = malloc(sizeof(pair) * num_partitions);
    for (int i = 0; i < num_partitions; i++) {
        pairs[i].key = NULL;
        pairs[i].value = NULL;
        pairs[i].next = NULL;
    }

    int i = 0;
    int multiple = 0;
    while (i < argc - 1) {
        for (i = 0 + multiple * num_mappers; i < num_mappers + (multiple * num_mappers); i++) {
            if (i == argc - 1) break;
            pthread_create(&mappers[i - multiple * num_mappers], NULL, (void *(*)(void *)) map, argv[i + 1]);
        }

        for (i = 0 + multiple * num_mappers; i < argc - 1 && i < num_mappers + (multiple * num_mappers); i++) {
            pthread_join(mappers[i - multiple * num_mappers], NULL);
        }
        multiple++;
    }

    /// TODO SORT
    ReduceArgs reduceArgs;
    reduceArgs.reduce = reduce;
    for (i = 0; i < num_reducers; i++) {
        reduceArgs.partition_number = i;
        pthread_create(&reducers[i], NULL, (void *(*)(void *)) ReduceThread, &reduceArgs);
    }
    /// TODO finish Getter function


    for (i = 0; i < num_reducers; i++) {
        pthread_join(reducers[i], NULL);
    }
}