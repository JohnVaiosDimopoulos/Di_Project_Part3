#include "Query.h"
#include <stdlib.h>
#include <string.h>
#include "../Util/Utilities.h"

struct Query {
  char *Relation;
  char *Predicates;
  char *Projection;

  Query_Ptr next;
};


Query_Ptr New_Query(char *buffer) {
  Query_Ptr Query = (Query_Ptr)malloc(sizeof(struct Query));
  Query->next = NULL;

  char* temp = Allocate_and_Copy_Str(buffer);
  char* token = strtok(temp, "|");
  for(int i = 0; i < 3; i++) {
    if (token == NULL) break;
    switch(i) {
      case 0:
        Query->Relation = Allocate_and_Copy_Str(token);
        break;
      case 1:
        Query->Predicates = Allocate_and_Copy_Str(token);
        break;
      case 2:
        Query->Projection = Allocate_and_Copy_Str(token);
        break;
    }
    token = strtok(NULL, "|");
  }
  free(temp);

  return Query;
}

void Print_Query(Query_Ptr Query) {
  printf("\t%s\n\t%s\n\t%s\n", Query->Relation, Query->Predicates, Query->Projection);
}

void Delete_Query(Query_Ptr Query) {
  free(Query->Relation);
  free(Query->Predicates);
  free(Query->Projection);
}


char* Get_Query_Relations(Query_Ptr Query) {
  return Query->Relation;
}

char* Get_Query_Predicates(Query_Ptr Query) {
  return Query->Predicates;
}

char* Get_Query_Projections(Query_Ptr Query) {
  return Query->Projection;
}

Query_Ptr Get_Next(Query_Ptr Query){
  return Query->next;
}

void Set_Next(Query_Ptr Node, Query_Ptr next){
  Node->next=next;
}

Query_Ptr Allocate_And_Copy_Query(Query_Ptr Query){
  Query_Ptr pnode = (Query_Ptr)malloc(sizeof(struct Query));
  pnode->Relation = Allocate_and_Copy_Str(Query->Relation );
  pnode->Predicates = Allocate_and_Copy_Str(Query->Predicates);
  pnode->Projection = Allocate_and_Copy_Str(Query->Projection);
  return pnode;
}
