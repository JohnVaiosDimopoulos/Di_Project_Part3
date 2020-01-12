#include <stdlib.h>
#include "Relation_Creator.h"
#include "../../../../Util/Utilities.h"


RelationPtr Create_Relation(int num_of_elements) {
  RelationPtr Relation = (RelationPtr)malloc(sizeof(Relation));

  if(Relation==NULL){
    printf("Memory Allocation error");
    exit(-1);
  }

  Relation->num_of_tuples = num_of_elements;
  Relation->tuples=Allocate_Array(Relation->num_of_tuples);
  if(Relation->tuples==NULL || Relation->num_of_tuples==0)
    return NULL;
  return Relation;

}

RelationPtr Create_Relation_with_given_array(int num_of_elements,Tuple_Ptr Array){
  RelationPtr Relation = (RelationPtr)malloc(sizeof(Relation));
  
  if(Relation==NULL){
    printf("Memory Allocation error");
    exit(-1);
  }

  Relation->num_of_tuples=num_of_elements;
  Relation->tuples=Array;
  return Relation;
}


static void Fill_array_from_file(RelationPtr Relation, FILE* File_Ptr){
  char* line_buffer = NULL;
  size_t line_buffer_size = 0;
  int lines_to_read = Relation->num_of_tuples;
  
  
  for(int i =0;i<lines_to_read;i++){
    int read = getline(&line_buffer,&line_buffer_size,File_Ptr);
    sscanf(line_buffer, "%llu, %llu", &Relation->tuples[i].element, &Relation->tuples[i].row_id);
  }
  free(line_buffer);

}

static int Count_File_elements(FILE* File_Ptr) {

  int num_of_elements = 0;

  for (int c = getc(File_Ptr);; c = getc(File_Ptr)){
    if (c == '\n')
      num_of_elements++;
    if (c == EOF) {
      num_of_elements++;
      break;
    }
  }
  rewind(File_Ptr);
  return num_of_elements;
};

RelationPtr Set_up_Relation(FILE* File_ptr) {
  int num_of_elements = Count_File_elements(File_ptr);
  RelationPtr Relation = Create_Relation(num_of_elements);

  if(Relation==NULL){
    printf("Error At Relation creation");
    exit(-1);
  }

  Fill_array_from_file(Relation, File_ptr);
//  Print_Relation(Relation);
  return Relation;
}

int Get_Num_of_elements(RelationPtr Relation){
  return Relation->num_of_tuples;
}

Tuple_Ptr Get_Tuples(RelationPtr Relation){
  return Relation->tuples;
}


void Delete_Relation(RelationPtr Relation){
  free(Relation->tuples);
  free(Relation);
}

void Print_Relation(RelationPtr Relation){
  for(int i=0;i<Relation->num_of_tuples;i++){
//    printf("(Row id: %lu),(Value: %lu)\n",Relation->tuples[i].row_id,Relation->tuples[i].element);
    printf("%d :(Row id: %llu),(Value: %llu)\n",i,Relation->tuples[i].row_id,Relation->tuples[i].element);
  }
  printf("\n\n");
}
