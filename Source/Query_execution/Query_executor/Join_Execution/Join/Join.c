#include "Join.h"
#include "../Relation_Creator/Relation_Creator.h"
#include "../../../../Util/Utilities.h"

static void Print_Result(List_Ptr List) {
  FILE *fp = fopen("./Results/output", "w");
  Print_List(List, fp);
  fclose(fp);
}



static List_Ptr Join_Relations(RelationPtr Relation_A, RelationPtr Relation_B) {

  Tuple_Ptr A, B1, B2;
  A = Relation_A->tuples;
  B1 = B2 = Relation_B->tuples;

  int i = 0;
  int cntA = 0;
  int cntB1 = 0;
  int cntB2 = 0;

  List_Ptr Results_List = Create_and_Initialize_List();

  while(1) {
    if(A->element < B2->element) {
      cntA++;
      if(cntA == Relation_A->num_of_tuples)
        break;
      A++;
      continue;
    }
    else if(A->element > B2->element) {
      cntB2++;
	  //reached end of B with B2
	  //move B2 back to B1
      if(cntB2 == Relation_B->num_of_tuples) {
        cntB2 = cntB1;
        B2 = B1;
		
		//and move A too
        cntA++;
        if(cntA == Relation_A->num_of_tuples)
          break;
        A++;
	  }else
        B2++;
      if(B1->element != B2->element) {
		cntB1 = cntB2;
        B1 = B2;
      }
      continue;
    }

    if(A->element == B2->element) {
      Insert_Record(Results_List, A->row_id, B2->row_id);
      i++;
    }

    cntB2++;
	//reached end of B with B2
	//move B2 back to B1
    if(cntB2 == Relation_B->num_of_tuples) {
      cntB2 = cntB1;
      B2 = B1;
      
	  //and move A too
      cntA++;
      if(cntA == Relation_A->num_of_tuples)
        break;
      A++;
	} else
      B2++;
	
	//if B changed element, move A too
    if(B1->element != B2->element) {
      cntA++;
      if(cntA == Relation_A->num_of_tuples)
        break;
      A++;
	  //if A changed element, move B1 too
      if(A->element != B1->element) {
		cntB1 = cntB2;
        B1 = B2;
	  //else move B2 back where B1 is
      } else {
		cntB2 = cntB1;
        B2 = B1;
      }
    }
  }
  return Results_List;
}


List_Ptr Execute_Join(RelationPtr Relation_A, RelationPtr Relation_B){
  List_Ptr Results_List = Join_Relations(Relation_A, Relation_B);
  return Results_List;
}

