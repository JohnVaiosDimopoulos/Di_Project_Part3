#ifndef SORT_JOIN_UTILITIES_H
#define SORT_JOIN_UTILITIES_H
#include <stdio.h>
#include <stdint.h>
#include "../Basis_Structs/Tuple.h"
char* Allocate_and_Copy_Str(const char*);
int Open_File_for_Read(FILE**,const char*);
int Open_File_for_Write(FILE**,const char*);
int Count_File_Lines(FILE *);
int generic_swap(void*,void*,int);
const char* construct_Path(const char* , const char*);
void Tokenize_to_rel_and_col(int *rel, int *col, char *str);
Tuple_Ptr Allocate_Array(int Array_elements);
#endif //SORT_JOIN_UTILITIES_H
