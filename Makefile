COMPILER = gcc
FLAGS = -Wall -std=c17
EX = main

all: compile

compile:
	$(COMPILER) $(FLAGS) main.c -o main

clean:
	rm -rf main

