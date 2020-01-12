#include "Utilities.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

char* Allocate_and_Copy_Str(const char* source){
  char* New_String=(char*)malloc(sizeof(char)*(strlen(source)+1));
  strcpy(New_String,source);
  return New_String;
}

static void Check_if_empty(FILE* File_ptr){
  fseek(File_ptr,0,SEEK_END);
  size_t size = ftell(File_ptr);
  if(0==size){
    printf("File is empty");
    exit(-1);
  }
  fseek(File_ptr,0,SEEK_SET);
}

int Open_File_for_Read(FILE** File_Ptr,const char* File_Name){
  if(((*File_Ptr)=fopen(File_Name,"r"))!=NULL){
    Check_if_empty(*File_Ptr);
    return 1;
  }
  printf("Error at file opening\n");
  exit(-1);
}

int Open_File_for_Write(FILE** File_Ptr,const char* File_Name){
  if(((*File_Ptr)=fopen(File_Name,"w"))!=NULL)
    return 1;
  printf("Error at file opening\n");
  perror("Error");
  exit(-1);
}


int Count_File_Lines(FILE *FilePtr) {
  int num_of_lines = 0;
  for(int c = getc(FilePtr);; c=getc(FilePtr)){
    if (c == '\n')
      num_of_lines++;
    if (c == EOF) {
      num_of_lines++;
      break;
    }
  }
  rewind(FilePtr);
  fclose(FilePtr);
  return num_of_lines;
}

int generic_swap(void* value_1, void* value_2, int size){
  void* temp;

   if((temp=malloc(size))==NULL)
    return -1;

  memcpy(temp,value_1,size);
  memcpy(value_1,value_2,size);
  memcpy(value_2,temp,size);
  free(temp);
  return 0;
}

const char* construct_Path(const char* FileName, const char* DirName){
  char* File_Path = (char*)malloc(sizeof(char)*(strlen(FileName)+strlen(DirName)+2));
  sprintf(File_Path,"%s/%s",DirName,FileName);
  return File_Path;
}


void Tokenize_to_rel_and_col(int *rel, int *col, char *str) {
  char *temp = (char*)malloc(strlen(str) * sizeof(char));
  strcpy(temp, str);
  char *tok = strtok(temp, ".");
  *rel = atoi(tok);
  tok = strtok(NULL, ".");
  *col = atoi(tok);

  free(temp);
}

Tuple_Ptr Allocate_Array(int Array_elements){

  if(Array_elements==0)
    return NULL;

  Tuple_Ptr New_Array = (Tuple_Ptr) malloc(Array_elements* sizeof(struct Tuple));

  if(New_Array==NULL){
    printf("Memory Allocation Failed\n");
    return NULL;
  }
  return New_Array;
}