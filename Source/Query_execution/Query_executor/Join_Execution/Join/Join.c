#include "Join.h"
#include "../Relation_Creator/Relation_Creator.h"
#include "../../../../Util/Utilities.h"

static void Print_Result(List_Ptr List) {
  FILE *fp = fopen("./Results/output", "w");
  Print_List(List, fp);
  fclose(fp);
}

//struct Tuple_List_Node {
//  uint64_t element;
//  uint64_t row_id;
//  struct Tuple_List_Node *next;
//};
//
////List of tuples
//struct Tuple_List {
//  struct Tuple_List_Node *start;
//  struct Tuple_List_Node *last;
//};
//
//typedef struct Tuple_List_Node* Tuple_List_Node_Ptr;
//typedef struct Tuple_List* Tuple_List_Ptr;

//static Tuple_List_Node_Ptr New_Node(Tuple_Ptr tuple) {
//  Tuple_List_Node_Ptr new = (Tuple_List_Node_Ptr)malloc(sizeof(struct Tuple_List_Node));
//  new->row_id = tuple->row_id;
//  new->element = tuple->element;
//  new->next = NULL;
//  return new;
//}
//static void Insert(Tuple_List_Ptr List, Tuple_Ptr tuple) {
//  Tuple_List_Node_Ptr new = New_Node(tuple);
//  Tuple_List_Node_Ptr pnode = List->last;
//  
//  //first node
//  if(pnode == NULL) {
//    List->start = new;
//  } else {
//    pnode->next = new;
//  }
//  List->last = new;
//}
//
//
//static Tuple_List_Ptr Create_List() {
//  Tuple_List_Ptr List = (Tuple_List_Ptr)malloc(sizeof(struct Tuple_List));
//  List->start  = NULL;
//  List->last  = NULL;
//  return List;
//}
//
//static void Print_Tuple_List(Tuple_List_Ptr List, uint64_t row_id, List_Ptr Result_List) {
//  //printf("PRINT LIST\n");
//  Tuple_List_Node_Ptr pnode = List->start;
//  while(pnode) {
//    Insert_Record(Result_List, row_id, pnode->row_id);
//    pnode = pnode->next;
//  }
//}

//static void Empty_List(Tuple_List_Ptr List) {
//  //printf("EMPTY LIST\n\n");
//  Tuple_List_Node_Ptr pnode = List->start;
//  Tuple_List_Node_Ptr temp;
//  while(pnode) {
//    temp = pnode->next;
//    free(pnode);
//    pnode = temp;
//  }
//  List->start = NULL;
//  List->last = NULL;
//}

//static void Join_Relations(RelationPtr A, RelationPtr B, List_Ptr Result_List) {
//  Tuple_Ptr A1, B1, B2;
//  A1 = A->tuples;
//  B1 = B2 = B->tuples;
//  Tuple_List_Ptr temp_List = Create_List();
//  
//  int cntA = 0;
//  int cntB = 0;
//  int end_of_B = 0;
//  while(1) {
//    if(end_of_B) {
//	  printf("end\n");
//      cntA++;
//      if(cntA == A->num_of_tuples) break;
//      A1++;
//      Print_Tuple_List(temp_List, A1->row_id, Result_List);
//      continue;
//    }
//    //move A
//    if(A1->element < B1->element) {
//      cntA++;
//      if(cntA == A->num_of_tuples) break;
//      A1++;
//      continue;
//    //move B
//    } else if(A1->element > B2->element) {
//      cntB++;
//      if(cntB == B->num_of_tuples) {
//        end_of_B = 1;
//      } else  {
//        B2++;
//      }
//      if(B1->element != B2->element) {
//        B1 = B2;
//      }
//      continue;
//      //all results in this element were printed
//      //move B1
//      //reset list
//      B1 = B2;
//      Empty_List(temp_List);
//    }
//    if(A1->element == B2->element) {
//      Insert_Record(Result_List, A1->row_id, B2->row_id);
//      Insert(temp_List, B2);
//    }
//    //still on the same element in B relation
//    if(B1->element == B2->element) {
//     //move B2 to find all results in this element
//     cntB++;
//     if(cntB == B->num_of_tuples) {
//       end_of_B = 1;
//     } else {
//       B2++;
//      }
//      //if B element changed
//      //all results were found
//      if(B1->element != B2->element) {
//        cntA++;
//        if(cntA == A->num_of_tuples) break;
//
//        A1++;
//        //changed element in A relation
//        //move B1
//        //reset list
//        if(A1->element != B1->element) {
//          B1 = B2;
//          Empty_List(temp_List);
//        //again same element in A relation
//        } else {
//          Print_Tuple_List(temp_List, A1->row_id, Result_List);
//          cntA++;
//          if(cntA == A->num_of_tuples) break;
//          A1++;
//        }
//      }
//      //B1 and B2 elements are different
//      //meaning we have found all results in this A-element
//      //we have to print them
//    } else {
//      if(A1->element == B1->element) {
//        Print_Tuple_List(temp_List, A1->row_id, Result_List);
//        cntA++;
//        if(cntA == A->num_of_tuples) break;
//        A1++;
//        if(A1->element > B1->element) {
//          B1 = B2;
//          Empty_List(temp_List);
//        }
//      }
//    }
//  }
//}

//static void Join_Relations(RelationPtr Relation_A, RelationPtr Relation_B, List_Ptr Result_List) {
static List_Ptr Join_Relations(RelationPtr Relation_A, RelationPtr Relation_B) {

  Tuple_Ptr A, B1, B2;
  A = Relation_A->tuples;
  B1 = B2 = Relation_B->tuples;

  int i = 0;
  int end_of_B = 0;
  int cntA = 0;
  int cntB1 = 0;
  int cntB2 = 0;

  List_Ptr Results_List = Create_and_Initialize_List();

  while(1) {
    if(end_of_B) {
	  printf("end\n");
      cntA++;
      if(cntA == Relation_A->num_of_tuples)
        break;
      A++;
      if(A->element == B2->element) {
        Insert_Record(Results_List, A->row_id, B2->row_id);
        i++;
      }
      //Print_Tuple_List(temp_List, A->row_id, Results_List);
      continue;
    }

    if(A->element < B2->element) {
      cntA++;
      if(cntA == Relation_A->num_of_tuples)
        break;
      A++;
      continue;
    }
    else if(A->element > B2->element) {
      cntB2++;
      if(cntB2 == Relation_B->num_of_tuples)
        end_of_B = 1;
	  else
        B2++;
      if(B1->element != B2->element) {
		cntB1 = cntB2;
        B1 =2;
      }
    }
    if(A->element == B2->element) {
      Insert_Record(Results_List, A->row_id, B2->row_id);
      i++;
    }
    cntB2++;
    if(cntB2 == Relation_B->num_of_tuples)
      end_of_B = 1;
	else
      B2++;
    if(B1->element != B2->element) {
      cntA++;
      if(cntA == Relation_A->num_of_tuples)
        break;
      A++;
      if(A->element != B1->element) {
		cntB1 = cntB2;
        B1 = B2;
      } else {
		cntB2 = cntB1;
        B2 = B1;
      }
    }
  }
  return Results_List;
}



List_Ptr Execute_Join(RelationPtr Relation_A, RelationPtr Relation_B){
  printf("JOIN\n\n");
  List_Ptr Results_List = Join_Relations(Relation_A, Relation_B);
  printf("JOIN ENDED\n\n");
  return Results_List;
}

