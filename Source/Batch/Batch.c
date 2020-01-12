#include "Batch.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

struct Batch {
  Query_Ptr Queries;
  Query_Ptr Last;
  int counter;
};

int Get_num_of_Queries(Batch_Ptr Batch) {
  return Batch->counter;
}


Batch_Ptr Create_Batch() {
  Batch_Ptr Batch = (Batch_Ptr)malloc(sizeof(struct Batch));
  Batch->counter = 0;
  Batch->Queries = NULL;
  Batch->Last = NULL;
  printf("=================New Batch ===================\n\n");
  return Batch;
}


void Insert_Query(char *buffer, Batch_Ptr Batch) {
  size_t len = strlen(buffer);
  //if list is empty first and last are the same
  if(Batch->counter == 0) {
    Batch->Queries = New_Query(buffer);
    Batch->Last = Batch->Queries;
  } else {
    Query_Ptr pnode = Batch->Last;
    Set_Next(pnode,New_Query(buffer));
    Batch->Last = Get_Next(pnode);
  }
  Batch->counter++;
}

void Print_Batch(Batch_Ptr Batch) {
  printf("\n\tPRINT\n");
  Query_Ptr pnode = Batch->Queries;
  for(int i = 0; i < Batch->counter; i++) {
    Print_Query(pnode);
    pnode = Get_Next(pnode);
  }
  printf("\n");
}

void Delete_Batch(Batch_Ptr Batch) {
  Query_Ptr node = Batch->Queries;
  Query_Ptr temp = node;
  for(int i = 0; i < Batch->counter; i++) {
    Delete_Query(node);

    node = Get_Next(node);
    free(temp);
    temp = node;
  }
  free(Batch);
}


Query_Ptr Pop_Next_Query_from_Batch(Batch_Ptr Batch) {
  if(Batch->counter == 0) {
    printf("LIST IS EMPTY, NO QUERY TO REMOVE\n");
    return NULL;
  }

  //allocate and copy query
  Query_Ptr pnode = Allocate_And_Copy_Query(Batch->Queries);

  //free the old one
  Query_Ptr temp = Batch->Queries;
  Batch->Queries=Get_Next(Batch->Queries);
  Delete_Query(temp);
  free(temp);

  Batch->counter--;
  return pnode;
}
