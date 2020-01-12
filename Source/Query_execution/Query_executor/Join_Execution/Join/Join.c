#include "Join.h"
#include "../Relation_Creator/Relation_Creator.h"
#include "../../../../Util/Utilities.h"

static void Print_Result(List_Ptr List) {
  FILE *fp = fopen("./Results/output", "w");
  Print_List(List, fp);
  fclose(fp);
}

static void Join_Relations(RelationPtr Relation_A, RelationPtr Relation_B, List_Ptr Result_List) {

  Tuple_Ptr A, B1, B2;
  A = Relation_A->tuples;
  B1 = B2 = Relation_B->tuples;

  int i = 0;
  int cntA = 0;

  while(1) {

    if(A->element < B2->element) {
      cntA++;
      if(cntA == Relation_A->num_of_tuples)
        break;
      A++;
      continue;
    }
    else if(A->element > B2->element) {
      B2++;
      if(B1->element != B2->element) {
        B1 = B2;
      }
      continue;
    }
    if(A->element == B2->element) {
      Insert_Record(Result_List, A->row_id, B2->row_id);
      i++;
    }
    B2++;
    if(B1->element != B2->element) {
      cntA++;
      if(cntA == Relation_A->num_of_tuples)
        break;
      A++;
      if(A->element != B1->element) {
        B1 = B2;
      } else {
        B2 = B1;
      }
    }
  }
}

List_Ptr Execute_Join(RelationPtr Relation_A, RelationPtr Relation_B){
  printf("JOIN\n\n");
  List_Ptr Result_List = Create_and_Initialize_List();
  Join_Relations(Relation_A, Relation_B, Result_List);
  printf("JOIN ENDED\n\n");
  return Result_List;
}

