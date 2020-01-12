#include "Parse_Projections.h"
#include "../../../Util/Utilities.h"
#include "../../Projection/Projection.h"
#include <stdlib.h>
#include <string.h>



static int Count_Projections(Query_Ptr Query) {
  char* temp = Allocate_and_Copy_Str(Get_Query_Projections(Query));
  char* token = strtok(temp, " ");
  int cnt = 0;

  while(token != NULL ) {
    cnt++;
    token = strtok(NULL, " ");
  }
  free(temp);
  return cnt;
}

static Projection_Ptr Fill_Projection_Array(Query_Ptr Query, int cnt) {
  //tokenize
  char *temp_proj[cnt];

  char *temp_query = Allocate_and_Copy_Str(Get_Query_Projections(Query));
  char *token = strtok(temp_query, " ");
  for(int i = 0; i < cnt; i++) {
    temp_proj[i] = (char*)malloc(strlen(token) * sizeof(char));
    strcpy(temp_proj[i], token);
    token = strtok(NULL, " ");
  }
  free(temp_query);

  Projection_Ptr proj_array = Allocate_Projection_Array(cnt);
  for(int i = 0; i < cnt; i++) {
    Projection_Ptr Proj = Get_Proj_by_index(proj_array,i);
    Tokenize_to_rel_and_col(Get_Projection_Relation(Proj), Get_Projection_Column(Proj), temp_proj[i]);
    free(temp_proj[i]);
  }

  return proj_array;
}

void Setup_Projections(Parsed_Query_Ptr Parsed_Query, Query_Ptr Query){
  int cnt = Count_Projections(Query);
  Projection_Ptr Projections_array;
  Set_num_of_projections(Parsed_Query,cnt);
  Projections_array = Fill_Projection_Array(Query, cnt);
  Set_Projections(Parsed_Query,Projections_array);
}