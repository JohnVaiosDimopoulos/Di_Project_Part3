#ifndef MULTI_JOIN_QUERY_PARSER_H
#define MULTI_JOIN_QUERY_PARSER_H

#include "../../Query/Query.h"
#include "../Join_Struct/Join_Struct.h"
#include "../Filter/Filter.h"
#include "../Projection/Projection.h"

typedef struct Parsed_Query* Parsed_Query_Ptr;

Parsed_Query_Ptr Parse_Query(Query_Ptr);
void Delete_Parsed_Query(Parsed_Query_Ptr Parsed_Query);
void Print_Parsed_Query(Parsed_Query_Ptr Parsed_Query);

//Parsed_Query Accessors//
Join_Ptr Get_Joins(Parsed_Query_Ptr Parsed_Query);
Filter_Ptr Get_Filters(Parsed_Query_Ptr Parsed_Query);
Projection_Ptr Get_Projections(Parsed_Query_Ptr Parsed_Query);
int Get_Num_of_Relations(Parsed_Query_Ptr);
int Get_Num_of_Projections(Parsed_Query_Ptr);
int* Get_Relations(Parsed_Query_Ptr);
int Get_Num_of_Filters(Parsed_Query_Ptr);
int Get_Num_of_Joins(Parsed_Query_Ptr);

//Parsed_Query Mutators
void Set_Relations(Parsed_Query_Ptr ,int*);
void Set_Num_of_Relations(Parsed_Query_Ptr ,int);
void Set_num_of_join(Parsed_Query_Ptr,int);
void Set_num_of_filters(Parsed_Query_Ptr,int);
void Set_num_of_projections(Parsed_Query_Ptr,int);
void Set_Joins(Parsed_Query_Ptr,Join_Ptr);
void Set_Filters(Parsed_Query_Ptr,Filter_Ptr);
void Set_Projections(Parsed_Query_Ptr,Projection_Ptr);


#endif //MULTI_JOIN_QUERY_PARSER_H
