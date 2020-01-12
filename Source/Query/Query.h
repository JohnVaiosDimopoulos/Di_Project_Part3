#ifndef MULTI_JOIN_QUERY_H
#define MULTI_JOIN_QUERY_H

typedef struct Query* Query_Ptr;
Query_Ptr New_Query(char*);
void Print_Query(Query_Ptr);
void Delete_Query(Query_Ptr);
char* Get_Query_Relations(Query_Ptr);
char* Get_Query_Predicates(Query_Ptr);
char* Get_Query_Projections(Query_Ptr);
Query_Ptr Get_Next(Query_Ptr Query);
void Set_Next(Query_Ptr, Query_Ptr);
Query_Ptr Allocate_And_Copy_Query(Query_Ptr);

#endif //MULTI_JOIN_QUERY_H
