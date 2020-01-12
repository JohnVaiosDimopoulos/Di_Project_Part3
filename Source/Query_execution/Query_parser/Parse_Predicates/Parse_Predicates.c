#include "Parse_Predicates.h"
#include "../../../Util/Utilities.h"
#include "../../Join_Struct/Join_Struct.h"
#include "../../Filter/Filter.h"
#include <string.h>
#include <stdlib.h>


static int Count_Predicates(Query_Ptr Query) {
  char* temp = Allocate_and_Copy_Str(Get_Query_Predicates(Query));
  char* token = strtok(temp, "&");
  int cnt = 0;

  while(token != NULL ) {
    cnt++;
    token = strtok(NULL, "&");
  }
  free(temp);
  return cnt;
}

static int it_is_Join(int *second_col, int *constant, char *predicates, char *type) {
  char *token;
  if(strstr(predicates, "<")) {
    token = strtok(predicates, "<");
    token = strtok(NULL, "<");
    *constant = atoi(token);
    strcpy(type, "<");
    return 0;
  } else if(strstr(predicates, ">")) {
    token = strtok(predicates, ">");
    token = strtok(NULL, ">");
    *constant = atoi(token);
    strcpy(type, ">");
    return 0;
  } else {
    token = strtok(predicates, "=");
    token = strtok(NULL, "=");
    if(strstr(token, ".")) {
      Tokenize_to_rel_and_col(&second_col[0], &second_col[1], token);
      return 1;
    } else {
      *constant = atoi(token);
      strcpy(type, "=");
      return 0;
    }
  }
}


static Parsed_Query_Ptr Fill_Predicates(Query_Ptr Query, int cnt, Parsed_Query_Ptr Parsed_Query) {
  char *temp_query = Allocate_and_Copy_Str(Get_Query_Predicates(Query));
  char *token = strtok(temp_query, "&");

  char *temp_pred[cnt];
  for(int i = 0; i < cnt; i++) {
    temp_pred[i] = (char*)malloc(strlen(token) * sizeof(char));
    strcpy(temp_pred[i], token);
    token = strtok(NULL, "&");
  }
  free(temp_query);

  int join_first_col[cnt][2];
  int filter_first_col[cnt][2];
  int second_col[cnt][2];
  char type_array[cnt][2];
  int constants[cnt];
  int num_of_filters = 0;
  int num_of_joins = 0;
  for(int i = 0; i < cnt; i++) {

    char *predicate = Allocate_and_Copy_Str(temp_pred[i]);
    char type[2];
    if(it_is_Join(second_col[num_of_joins], &constants[num_of_filters], predicate,type)) {
      Tokenize_to_rel_and_col(&join_first_col[num_of_joins][0], &join_first_col[num_of_joins][1], temp_pred[i]);
      num_of_joins++;
    } else {
      Tokenize_to_rel_and_col(&filter_first_col[num_of_filters][0], &filter_first_col[num_of_filters][1], temp_pred[i]);
      strcpy(type_array[num_of_filters], type);
      num_of_filters++;
    }

    free(temp_pred[i]);
    free(predicate);
  }

  //joins
  Join_Ptr Join_array;
  Join_array = Allocate_Join_Array(num_of_joins);
  for(int i = 0; i < num_of_joins; i++) {
    Join_Ptr Join = Get_Join_by_index(Join_array,i);
    Set_Relation_1(join_first_col[i][0],Join);
    Set_Column_1(join_first_col[i][1],Join);
    Set_Relation_2(second_col[i][0],Join);
    Set_Column_2(second_col[i][1],Join);
  }
  Set_num_of_join(Parsed_Query,num_of_joins);
  Set_Joins(Parsed_Query,Join_array);

  //filters
  Filter_Ptr Filters_array;
  Filters_array = Allocate_filter_Array(num_of_filters);

  for(int i = 0; i < num_of_filters; i++) {
    Filter_Ptr Filter = Get_Filter_by_index(Filters_array,i);
    Set_Relation(Filter,filter_first_col[i][0]);
    Set_Column(Filter,filter_first_col[i][1]);
    Set_constant(Filter,constants[i]);
    Set_type(Filter,type_array[i]);
  }
  Set_num_of_filters(Parsed_Query,num_of_filters);
  Set_Filters(Parsed_Query,Filters_array);
  return Parsed_Query;
}

void Setup_Joins_And_Filters(Parsed_Query_Ptr Parsed_Query, Query_Ptr Query){
  int cnt = Count_Predicates(Query);
  Parsed_Query = Fill_Predicates(Query, cnt, Parsed_Query);
}
