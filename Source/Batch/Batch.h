#ifndef MULTI_JOIN_BATCH_H
#define MULTI_JOIN_BATCH_H

#include "../Query/Query.h"

typedef struct Batch* Batch_Ptr;
void Delete_Batch(Batch_Ptr);
Batch_Ptr Create_Batch();
int Get_num_of_Queries(Batch_Ptr);
Query_Ptr Pop_Next_Query_from_Batch(Batch_Ptr);
void Insert_Query(char *, Batch_Ptr);
void Print_Batch(Batch_Ptr);



#endif //MULTI_JOIN_BATCH_H
