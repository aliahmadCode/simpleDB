#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>


#define COL_USERNAME_SIZE 32
#define COL_EMAIL_SIZE 255

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)


void show_error(const char *err){
    perror(err);
    puts("\n");
    exit(EXIT_FAILURE);
}

// .* are meta commands in NO SQL database
// themetac
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum {PREPARE_SUCCESS, PREPARE_UNRECOGNUIZED_STATE, PREPARE_SYNTAX_ERROR} PrepareResult;

// thetokens
typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;


// thestructs
typedef struct {
    uint32_t id;
    char username[COL_USERNAME_SIZE];
    char email[COL_EMAIL_SIZE];
} Row;

// theconsts
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t MAX_PAGE = 4096;
// table max pages
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = MAX_PAGE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
} Table;


typedef struct {
    StatementType type;
    Row row;
} Statement;

typedef struct {
    char *buffer;
    size_t input_length;
    size_t buffer_length;
} Input_Buffer;



// thefuncs
Input_Buffer *get_input_buffer(){
    Input_Buffer *new_input_buffer = (Input_Buffer*)malloc(sizeof(Input_Buffer));
    new_input_buffer->buffer = NULL;
    new_input_buffer->input_length = 0;
    new_input_buffer->buffer_length = 0;
    return new_input_buffer;
}

void close_input_buffer(Input_Buffer*input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

void print_prompt(){
    printf("db > ");
}

void read_input(Input_Buffer * input_buffer){
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0){
        show_error("getline failed");
    }


    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = '\0';
}

int do_meta_command(char *buffer){
    if(strcmp(buffer, ".exit") == 0){
        exit(EXIT_SUCCESS);
    }else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

int prepare_statement(Input_Buffer *input_buffer, Statement *statement){
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s", &(statement->row.id), statement->row.username, statement->row.email);
        if (args_assigned < 3){
            return PREPARE_SYNTAX_ERROR;

        }
        return PREPARE_SUCCESS;
    }
    if(strncmp(input_buffer->buffer, "select", 6) == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNUIZED_STATE;
}

void execute_commnad(Statement *statement){
    switch (statement->type) {
        case (STATEMENT_INSERT):
            printf("%d %s %s\n", statement->row.id, statement->row.username, statement->row.email);
            break;
        case (STATEMENT_SELECT):
            printf("This is where we would do select\n");
            break;
    }
}

void serialize_row(Row *source, void *destination){
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination){
    memcpy(&(destination->id), source + ID_OFFSET,  ID_SIZE);
    memcpy(&(destination->username),source + USERNAME_OFFSET,  USERNAME_SIZE);
    memcpy(&(destination->email),source + EMAIL_OFFSET,  EMAIL_SIZE);
}

// home
int main(int argc, char *argv[]){
    Input_Buffer* input_buffer = get_input_buffer();

    while (true) {
        print_prompt();
        read_input(input_buffer);

        if(input_buffer->buffer[0] == '.'){
            switch (do_meta_command(input_buffer->buffer)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_UNRECOGNUIZED_STATE):
                printf("Unrecognized keyword at the start of '%s'\n", input_buffer->buffer);
                continue;
        }

        execute_commnad(&statement);
        printf("Executed\n");
    }
    return EXIT_SUCCESS;
}
