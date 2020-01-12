#include "Parse_Relations.h"
#include "../../../Util/Utilities.h"
#include <stdlib.h>
#include <string.h>


static int Count_Relations(Query_Ptr Query) {
  char* temp = Allocate_and_Copy_Str(Get_Query_Relations(Query));
  char* token = strtok(temp, " ");
  int cnt = 0;

  while(token != NULL ) {
    cnt++;
    token = strtok(NULL, " ");
  }
  free(temp);
  return cnt;
}

static int* Convert_Relations_to_Ints(Query_Ptr Query, int cnt) {
  int *rel = (int*)malloc(cnt * sizeof(int));

  char *temp = Allocate_and_Copy_Str(Get_Query_Relations(Query));
  char *token = strtok(temp, " ");
  for(int i = 0; i < cnt; i++) {
    rel[i] = atoi(token);
    token = strtok(NULL, " ");
  }
  free(temp);
  return rel;
}

void Setup_Relations(Parsed_Query_Ptr Parsed_Query, Query_Ptr Query){
  int cnt = Count_Relations(Query);
  int* rel = Convert_Relations_to_Ints(Query, cnt);
  Set_Relations(Parsed_Query,rel);
  Set_Num_of_Relations(Parsed_Query,cnt);
}