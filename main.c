#define _POSIX_C_SOURCE 200809L // tells the compiler to enable the POXIS_C features
// should be defined before the <stdio.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>


#define COL_USERNAME_SIZE 32
#define COL_EMAIL_SIZE 255


// .* are meta commands in NO SQL database
// themetac
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;
typedef enum {EXECUTE_SUCCESS, EXECUTE_TABLE_FULL} ExecuteResult;
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

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);

const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
// table max pages
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
} Table;


typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

typedef struct {
    char *buffer;
    size_t input_length;
    size_t buffer_length;
} Input_Buffer;


// thefuncs

void show_error(const char *err){
    perror(err);
    puts("\n");
    exit(EXIT_FAILURE);
}

Table *new_table(){
    Table *table = (Table*)malloc(sizeof(Table));
    table->num_rows = 0;
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        table->pages[i] = NULL;
    }
    return table;
}

void free_table(Table *table){
    for(uint32_t i = 0; table->pages[i]; i++){
        free(table->pages[i]);
    }
    free(table);
}

void print_row(Row *row){
    printf("(%d %s %s)\n", row->id, row->username, row->email);
}


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

    if (bytes_read == -1){
        show_error("getline failed");
    }

    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = '\0';
}

MetaCommandResult do_meta_command(Input_Buffer *input_buffer, Table *table){
    if(strcmp(input_buffer->buffer, ".exit") == 0){
        close_input_buffer(input_buffer);
        free_table(table);
        exit(EXIT_SUCCESS);
    }else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(Input_Buffer *input_buffer, Statement *statement){
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id), statement->row_to_insert.username, statement->row_to_insert.email);

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

// to figure out where to read/write in memory
// one of best function i have ever read
void *row_slot(Table *table, uint32_t row_num){
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];

    if (page == NULL){
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

ExecuteResult execute_insert(Statement *statement, Table *table){
    if(table->num_rows >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }
    Row *row_to_insert = &(statement->row_to_insert);
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table){
    Row row;
    if(table->num_rows > 0){
        for(uint32_t i = 0; i < table->num_rows; i++){
            deserialize_row(row_slot(table, i), &row);
            print_row(&row);
        }
    }else{
        printf("db is empty\n");
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table){
    switch (statement->type){
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
    // ingnore this warning for right-now
}


// home
int main(int argc, char *argv[]){
    Input_Buffer* input_buffer = get_input_buffer();
    Table *table = new_table();

    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (strcmp(input_buffer->buffer, "") == 0) continue;

        if(input_buffer->buffer[0] == '.'){
            switch (do_meta_command(input_buffer, table)) {
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
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax Error. Could'nt parse the statement\n");
                continue;
            case (PREPARE_UNRECOGNUIZED_STATE):
                printf("Unrecognized keyword at the start of '%s'\n", input_buffer->buffer);
                continue;
        }

        switch(execute_statement(&statement, table)){
            case (EXECUTE_SUCCESS):
                printf("Executed\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Table Full\n");
                break;

        }
    }
    return EXIT_SUCCESS;
}
