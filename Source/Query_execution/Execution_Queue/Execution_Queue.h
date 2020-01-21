#ifndef MULTI_JOIN_EXECUTION_QUEUE_H
#define MULTI_JOIN_EXECUTION_QUEUE_H

#include "../Query_parser/Query_parser.h"
#include <stdint.h>

typedef struct Execution_Queue* Execution_Queue_Ptr;
typedef struct Execution_Queue_Node* Execution_Queue_Node_Ptr;
typedef struct Stats* Stats_Ptr;

Execution_Queue_Ptr Create_Execution_Queue();
int Is_Empty(Execution_Queue_Ptr);
void Delete_Queue(Execution_Queue_Ptr Execution_Queue);
void Insert_Node(Join_Ptr, Execution_Queue_Ptr, Stats_Ptr, Stats_Ptr, uint64_t, uint64_t);
//void Insert_Node(Join_Ptr, Execution_Queue_Ptr);
Join_Ptr Pop_Next_join(Execution_Queue_Ptr);
int Is_in_Queue(Join_Ptr,Execution_Queue_Ptr);
int Connects_with_last_join(Join_Ptr,Execution_Queue_Ptr);
void Print_Queue(Execution_Queue_Ptr);


void Set_Join_stats1_l(Execution_Queue_Node_Ptr, int, uint64_t);
void Set_Join_stats1_u(Execution_Queue_Node_Ptr, int, uint64_t);
void Set_Join_stats1_f(Execution_Queue_Node_Ptr, int, int64_t);
void Set_Join_stats1_d(Execution_Queue_Node_Ptr, int, int64_t);

void Set_Join_stats2_l(Execution_Queue_Node_Ptr, int, uint64_t);
void Set_Join_stats2_u(Execution_Queue_Node_Ptr, int, uint64_t);
void Set_Join_stats2_f(Execution_Queue_Node_Ptr, int, int64_t);
void Set_Join_stats2_d(Execution_Queue_Node_Ptr, int, int64_t);

uint64_t Get_Join_stats1_l(Execution_Queue_Node_Ptr, int);
uint64_t Get_Join_stats1_u(Execution_Queue_Node_Ptr, int);
int64_t Get_Join_stats1_f(Execution_Queue_Node_Ptr, int);
int64_t Get_Join_stats1_d(Execution_Queue_Node_Ptr, int);

uint64_t Get_Join_stats2_l(Execution_Queue_Node_Ptr, int);
uint64_t Get_Join_stats2_u(Execution_Queue_Node_Ptr, int);
int64_t Get_Join_stats2_f(Execution_Queue_Node_Ptr, int);
int64_t Get_Join_stats2_d(Execution_Queue_Node_Ptr, int);

Stats_Ptr Get_Node_stats1(Execution_Queue_Node_Ptr);
Stats_Ptr Get_Node_stats2(Execution_Queue_Node_Ptr);
uint64_t Get_Node_num_of_columns1(Execution_Queue_Node_Ptr);
uint64_t Get_Node_num_of_columns2(Execution_Queue_Node_Ptr);

#endif //MULTI_JOIN_EXECUTION_QUEUE_H
