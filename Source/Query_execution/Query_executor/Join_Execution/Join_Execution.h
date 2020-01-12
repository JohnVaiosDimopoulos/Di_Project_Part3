#ifndef MULTI_JOIN_JOIN_EXECUTION_H
#define MULTI_JOIN_JOIN_EXECUTION_H

#include "../../Execution_Queue/Execution_Queue.h"
#include "../Filter_Executor/Filter_Executor.h"
#include "../../../Basis_Structs/Relation.h"

struct Result{
  int relation;
  uint64_t row_id;
};


struct Intermediate_Result{
  int relations_in_result[4];
  int num_of_results;
  int num_of_relations;
  struct Result** row_ids;
  RelationPtr old_Relation1;
  RelationPtr old_Relation2;
};

typedef struct Intermediate_Result* Intermediate_Result_Ptr;


Intermediate_Result_Ptr Execute_Joins(Execution_Queue_Ptr Execution_Queue, Table_Ptr Filtered_Relations,Table_Ptr,int*);
void Print_Intermediate(Intermediate_Result_Ptr Intermediate_Result);
void Delete_intermediate_Result(Intermediate_Result_Ptr Intermediate_Result);



#endif //MULTI_JOIN_JOIN_EXECUTION_H
