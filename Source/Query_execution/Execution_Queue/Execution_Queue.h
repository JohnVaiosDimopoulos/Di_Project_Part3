#ifndef MULTI_JOIN_EXECUTION_QUEUE_H
#define MULTI_JOIN_EXECUTION_QUEUE_H

#include "../Query_parser/Query_parser.h"

typedef struct Execution_Queue* Execution_Queue_Ptr;

Execution_Queue_Ptr Create_Execution_Queue();
int Is_Empty(Execution_Queue_Ptr);
void Delete_Queue(Execution_Queue_Ptr Execution_Queue);
void Insert_Node(Join_Ptr Join, Execution_Queue_Ptr);
Join_Ptr Pop_Next_join(Execution_Queue_Ptr);
int Is_in_Queue(Join_Ptr,Execution_Queue_Ptr);
int Connects_with_last_join(Join_Ptr,Execution_Queue_Ptr);
void Print_Queue(Execution_Queue_Ptr);


#endif //MULTI_JOIN_EXECUTION_QUEUE_H
