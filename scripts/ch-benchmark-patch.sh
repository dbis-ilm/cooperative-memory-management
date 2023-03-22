#!/bin/sh
sed -i 's/LDFLAGS=-lodbc$/LDFLAGS=-lodbc -pthread/g' Makefile