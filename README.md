# Concurrent MapReduce

## Overview
This project implements a concurrent MapReduce framework in C. It allows for parallel processing of data using multiple mapper and reducer threads.
We have also provided a Wordcount example from the original [MapReduce paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

## Compilation
To compile the project, run:
```sh
make
```

To compile the project with debug symbols and no optimization, run:
```sh
make debug
```

## Usage
To run the MapReduce program, use the following command:
```sh
./concurrent_map_reduce <input_files>
```

## Example
```sh
./concurrent_map_reduce tests/1.txt tests/2.txt
```
