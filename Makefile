

COMPILER = gcc
FLAGS = -Wall -std=c17
EX = main

all:
	$(COMPILER) $(FLAGS) main.c -o main
	./$(EX)

clean:
	rm -rf main


