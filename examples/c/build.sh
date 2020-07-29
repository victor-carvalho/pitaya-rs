#!/bin/bash

OUTPUT_LIB=../../target/debug/
HEADER_PATH=../../src/pitaya/

clang -o example main.c -I $HEADER_PATH -I nanopb/ -L $OUTPUT_LIB -l pitaya
