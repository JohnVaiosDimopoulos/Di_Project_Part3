#ifndef MULTI_JOIN_JOIN_EXECUTION_H
#define MULTI_JOIN_JOIN_EXECUTION_H

#include "../../Execution_Queue/Execution_Queue.h"
#include "../Filter_Executor/Filter_Executor.h"
#include "../../../Basis_Structs/Relation.h"
#include "Columns_Used_List/Columns_Used_List.h"

struct Result{
  int relation;
  uint64_t row_id;
};




struct Intermediate_Result{
  int relations_in_result[4];
  int num_of_results;
  int num_of_relations;
  struct Result** row_ids;
  Used_Columns_List_Ptr Used_Columns;

};

typedef struct Intermediate_Result* Intermediate_Result_Ptr;


Intermediate_Result_Ptr Execute_Joins(Execution_Queue_Ptr Execution_Queue, Table_Ptr Filtered_Relations,Table_Ptr,int*);
void Print_Intermediate(Intermediate_Result_Ptr Intermediate_Result, char *name);
void Delete_intermediate_Result(Intermediate_Result_Ptr Intermediate_Result);



#endif //MULTI_JOIN_JOIN_EXECUTION_H
