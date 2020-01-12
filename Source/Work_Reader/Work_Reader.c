#include "Work_Reader.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


Batch_Ptr Read_next_Batch(FILE *fp) {
  char *line_buffer = NULL;
  size_t line_buffer_size = 0;

  int read = getline(&line_buffer, &line_buffer_size, fp);
  if(read < 0) {
    printf("Reached EOF\n");
    free(line_buffer);
    return NULL;
  }
  Batch_Ptr Batch = Create_Batch();

  //read batch line by line
  while(1) {
    printf("%s\n", line_buffer);

    //if line was "F" batch is over
	if(!strcmp(line_buffer, "F\n")) { printf("F found\n"); break;}

    char* command = malloc(sizeof(char) * line_buffer_size);
    sprintf(command, "%s", line_buffer);
    Insert_Query(command, Batch);
    free(command);
    
	read = getline(&line_buffer, &line_buffer_size, fp);
  }
  free(line_buffer);


//  Print_Batch(Batch);
  return Batch;
}





