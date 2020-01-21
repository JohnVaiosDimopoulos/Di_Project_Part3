#ifndef MULTI_JOIN_TABLE_ALLOCATOR_H
#define MULTI_JOIN_TABLE_ALLOCATOR_H

#include "../Argument_Data/Arguments_Data.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

typedef struct Table_Allocator* Table_AllocatorPtr;
typedef struct Table* Table_Ptr;
typedef struct Shell* Shell_Ptr;
typedef struct Tuple* Tuple_Ptr;
typedef struct Column_Stats* Column_Stats_Ptr;

Table_AllocatorPtr Create_Table_Allocator(Argument_Data_Ptr);
Table_Ptr Allocate_Table(Table_AllocatorPtr Table_Allocator);
Table_Ptr Allocate_Table_with_num_of_Shells(int);
void Fill_Table(Table_Ptr Table, Table_AllocatorPtr Table_Allocator);

void Print_Table(Table_Ptr);
void Allocate_Shell(Shell_Ptr);

void Set_Shell_Array(Shell_Ptr, Tuple_Ptr *);
void Set_Shell_num_of_tuples(Shell_Ptr, int);

Shell_Ptr Get_Table_Array(Table_Ptr);
int Get_num_of_shells(Table_Ptr);
Shell_Ptr Get_Shell_by_index(Shell_Ptr,int);

uint64_t Get_num_of_tuples(Shell_Ptr);
uint64_t Get_num_of_columns(Shell_Ptr);
Tuple_Ptr* Get_Shell_Array(Shell_Ptr);
Tuple_Ptr Get_Shell_Array_by_index(Shell_Ptr, int, int);
Table_Ptr Make_Table_For_Joins(Table_Ptr Relations, int* relations,int num_of_relations);
//Tuple_Ptr Get_Shell_Array_by_index(Tuple_Ptr, int);

uint64_t Get_Data(Tuple_Ptr);
uint64_t Get_Row_id(Tuple_Ptr);
Tuple_Ptr Get_Column(Shell_Ptr Shell,int column_id);
void Setup_Column_Pointers(Tuple_Ptr* Array,int num_of_columns,int num_of_tuples);
void Delete_Table_Allocator(Table_AllocatorPtr);
void Delete_Table(Table_Ptr);
//void Print_Relation(Tuple_Ptr*,int,int);

uint64_t Get_Shell_stats(Shell_Ptr);
uint64_t Get_Column_l(Shell_Ptr, uint64_t);
uint64_t Get_Column_u(Shell_Ptr, uint64_t);
int64_t Get_Column_f(Shell_Ptr, uint64_t);
int64_t Get_Column_d(Shell_Ptr, uint64_t);
bool** Get_d_array(Shell_Ptr);

void Set_Column_l(Shell_Ptr, uint64_t, uint64_t);
void Set_Column_u(Shell_Ptr, uint64_t, uint64_t);
void Set_Column_f(Shell_Ptr, uint64_t, int64_t);
void Set_Column_d(Shell_Ptr, uint64_t, int64_t);

void Print_Shell(Shell_Ptr Shell, FILE *fp);

#endif //MULTI_JOIN_TABLE_ALLOCATOR_H
