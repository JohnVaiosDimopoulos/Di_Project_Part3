#include "Query_parser.h"
#include "../../Util/Utilities.h"
#include "./Parse_Projections/Parse_Projections.h"
#include "./Parse_Predicates/Parse_Predicates.h"
#include "./Parse_Relations/Parse_Relations.h"
#include <stdlib.h>
#include <string.h>

struct Parsed_Query{
  int* relations;
  int num_of_relations;

  Join_Ptr Joins;
  int num_of_joins;

  Filter_Ptr Filters;
  int num_of_filters;

  Projection_Ptr Projections;
  int num_of_projections;
};


static Parsed_Query_Ptr Allocate_Parsed_query(){
  Parsed_Query_Ptr Parsed_Query = (Parsed_Query_Ptr)malloc(sizeof(struct Parsed_Query));
//  Parsed_Query->Filters=NULL;
//  Parsed_Query->Joins=NULL;
//  Parsed_Query->relations=NULL;
//  Parsed_Query->Projections=NULL;
  return Parsed_Query;
}

void Delete_Parsed_Query(Parsed_Query_Ptr Parsed_Query){
  free(Parsed_Query->relations);
  free(Parsed_Query->Joins);
  free(Parsed_Query->Filters);
  free(Parsed_Query->Projections);
  free(Parsed_Query);
}


void Print_Parsed_Query(Parsed_Query_Ptr Parsed_Query) {
  printf("===RELATIONS===\n");
  for (int i = 0; i < Parsed_Query->num_of_relations; i++) {
    printf("%d:%d \t", i, Parsed_Query->relations[i]);
  }
  printf("\n");

  printf("===JOINS===\n");
  for (int i = 0; i < Parsed_Query->num_of_joins; i++) {
    Join_Ptr Join = Get_Join_by_index(Parsed_Query->Joins,i);
    Print_Join(Join);
  }
  printf("\n");

  printf("===FILTERS===\n");
  if (Parsed_Query->num_of_filters != 0) {
    for (int i = 0; i < Parsed_Query->num_of_filters; i++) {
      Filter_Ptr Filter= Get_Filter_by_index(Parsed_Query->Filters,i);
      Print_Filter(Filter);
    }
    printf("\n");
  }

  printf("===PROJECTIONS===\n");
  for (int i = 0; i < Parsed_Query->num_of_projections; i++) {
    Projection_Ptr Projection = Get_Proj_by_index(Parsed_Query->Projections,i);
    Print_Projection(Projection);
  }
  printf("\n\n");

}


Parsed_Query_Ptr Parse_Query(Query_Ptr Query){
  Parsed_Query_Ptr Parsed_Query = Allocate_Parsed_query();
  //relations
  Setup_Relations(Parsed_Query, Query);

  //joins and filters
  Setup_Joins_And_Filters(Parsed_Query,Query);

  //projections
  Setup_Projections(Parsed_Query,Query);
  Print_Parsed_Query(Parsed_Query);
  return Parsed_Query;
}



//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////

//Parsed Query Accessors//

Join_Ptr Get_Joins(Parsed_Query_Ptr Parsed_Query){
  return Parsed_Query->Joins;
}

Projection_Ptr Get_Projections(Parsed_Query_Ptr Parsed_Query){
  return Parsed_Query->Projections;
}

int Get_Num_of_Joins(Parsed_Query_Ptr Parsed_Query){
  return Parsed_Query->num_of_joins;
}

Filter_Ptr Get_Filters(Parsed_Query_Ptr Parsed_Query){
  return Parsed_Query->Filters;
}

int Get_Num_of_Filters(Parsed_Query_Ptr Parsed_Query){
  return Parsed_Query->num_of_filters;
}

int* Get_Relations(Parsed_Query_Ptr Parsed_Query){
  return Parsed_Query->relations;
}

int Get_Num_of_Relations(Parsed_Query_Ptr Parsed_Query){
  return Parsed_Query->num_of_relations;
}

int Get_Num_of_Projections(Parsed_Query_Ptr Parsed_Query){
  return Parsed_Query->num_of_projections;
}

//Parsed Query Mutators//
void Set_Relations(Parsed_Query_Ptr Parsed_Query ,int* rel){
  Parsed_Query->relations=rel;
}

void Set_Num_of_Relations(Parsed_Query_Ptr Parsed_Query ,int num_of_rel){
  Parsed_Query->num_of_relations=num_of_rel;
}

void Set_num_of_join(Parsed_Query_Ptr Parsed_Query ,int num_of_join){
  Parsed_Query->num_of_joins=num_of_join;
}

void Set_num_of_filters(Parsed_Query_Ptr Parsed_Query,int num_of_filters){
  Parsed_Query->num_of_filters=num_of_filters;
}

void Set_num_of_projections(Parsed_Query_Ptr Parsed_Query,int num_of_projections){
  Parsed_Query->num_of_projections=num_of_projections;
}

void Set_Joins(Parsed_Query_Ptr Parsed_Query,Join_Ptr Join){
  Parsed_Query->Joins=Join;
}

void Set_Filters(Parsed_Query_Ptr Parsed_Query,Filter_Ptr Filter){
  Parsed_Query->Filters=Filter;
}

void Set_Projections(Parsed_Query_Ptr Parsed_Query,Projection_Ptr Projections){
  Parsed_Query->Projections=Projections;
}
