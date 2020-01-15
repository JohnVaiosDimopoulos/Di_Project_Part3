#include "Preparator.h"
#include "../Execution_Queue/Execution_Queue.h"
#include "../Join_Struct/Join_Struct.h"
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

typedef struct HT {
  Rel_Queue_Ptr *Table;
}HT;

struct Rel_Queue{
  struct Rel_Queue_Node* head;
  struct Rel_Queue_Node* tail;
};

struct Rel_Queue_Node{
  int rel;
  struct Rel_Queue_Node* next;
};

typedef struct Rel_Queue_Node* Rel_Queue_Node_Ptr;

static void Check_For_Self_joins(Parsed_Query_Ptr Parsed_Query,Execution_Queue_Ptr Execution_Queue,int* joins_inserted){
  Join_Ptr Joins_Array = Get_Joins(Parsed_Query);

  for(int i =0;i<Get_Num_of_Joins(Parsed_Query);i++){
    Join_Ptr Current_Join = Get_Join_by_index(Joins_Array,i);
    if(Is_Self_Join(Current_Join)){
      Insert_Node(Current_Join,Execution_Queue);
      (*joins_inserted)++;

    }
  }
}

static void Check_For_Same_Column_joins(Parsed_Query_Ptr Parsed_Query,Execution_Queue_Ptr Execution_Queue,int* joins_inserted){
  Join_Ptr Joins_Array = Get_Joins(Parsed_Query);

  for(int i =0;i<Get_Num_of_Joins(Parsed_Query);i++){
    Join_Ptr Current_Join = Get_Join_by_index(Joins_Array,i);
    if(Is_in_Queue(Current_Join,Execution_Queue))
      continue;

    for(int j =0;j<Get_Num_of_Joins(Parsed_Query);j++){
      Join_Ptr Next_Join = Get_Join_by_index(Joins_Array,j);
      if(Is_in_Queue(Next_Join,Execution_Queue))
        continue;
      if(Is_Same_Column_used(Current_Join,Next_Join) && !Is_the_Same_Join(Current_Join,Next_Join)){
        if(!Is_in_Queue(Current_Join,Execution_Queue)){
          Insert_Node(Current_Join,Execution_Queue);
          (*joins_inserted)++;
        }
        Insert_Node(Next_Join,Execution_Queue);
        (*joins_inserted)++;
      }
    }
  }
}

static void Organize_Joins(Parsed_Query_Ptr Parsed_Query,Execution_Queue_Ptr Execution_Queue,int* joins_inserted){
  Join_Ptr Join_Array = Get_Joins(Parsed_Query);

  for(int i =0;i<Get_Num_of_Joins(Parsed_Query);i++){
    Join_Ptr Current_Join = Get_Join_by_index(Join_Array,i);

    if(Is_Empty(Execution_Queue)){
      Insert_Node(Current_Join,Execution_Queue);
      continue;
    }

    if(Is_in_Queue(Current_Join,Execution_Queue))
      continue;
    if(Connects_with_last_join(Current_Join,Execution_Queue)){
      Insert_Node(Current_Join,Execution_Queue);
      (*joins_inserted)++;
    }

  }
}

static void Fill_the_rest(Parsed_Query_Ptr Parsed_Query,Execution_Queue_Ptr Execution_Queue,int* joins_inserted){
  if((*joins_inserted)==Get_Num_of_Joins(Parsed_Query))
    return;

  Join_Ptr Join_Array = Get_Joins(Parsed_Query);

  for(int i =0;i<Get_Num_of_Joins(Parsed_Query);i++){
    Join_Ptr Current_Join = Get_Join_by_index(Join_Array,i);

    if(Is_in_Queue(Current_Join,Execution_Queue))
      continue;

    Insert_Node(Current_Join,Execution_Queue);
    (*joins_inserted)++;

  }
}

///////////////////////////////////////////////		NEW		///////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////
static void Print_Rel_Queue(Rel_Queue_Ptr Rel_Queue){
  printf("===REL QUEUE===\n");
  Rel_Queue_Node_Ptr temp = Rel_Queue->head;
  while (temp!=NULL){
    printf("(%d)->", temp->rel);
    temp=temp->next;
  }
  printf("\n\n");
}
static void Delete_Rel_Queue(Rel_Queue_Ptr Rel_Queue){
  Rel_Queue_Node_Ptr current = Rel_Queue->head;
  Rel_Queue->head=NULL;
  Rel_Queue->tail=NULL;
  while(current!=NULL){
    Rel_Queue_Node_Ptr temp = current;
    current=temp->next;
    free(temp);
  }
  free(Rel_Queue);
}

static Rel_Queue_Ptr Create_Rel_Queue(){
  Rel_Queue_Ptr Rel_Queue = (Rel_Queue_Ptr)malloc(sizeof(struct Rel_Queue));
  Rel_Queue->head=NULL;
  Rel_Queue->tail=NULL;
  return Rel_Queue;
}

static Rel_Queue_Node_Ptr Create_New_Node(int rel){
  Rel_Queue_Node_Ptr new_node = (Rel_Queue_Node_Ptr)malloc(sizeof(struct Rel_Queue_Node));
  new_node->rel = rel;
  new_node->next = NULL;
  return new_node;

}

static void Insert_In_Empty_Queue(int rel, Rel_Queue_Ptr Rel_Queue){
  Rel_Queue->head = Create_New_Node(rel);
  Rel_Queue->tail = Rel_Queue->head;
}

static void Insert_At_End(int rel, Rel_Queue_Ptr Rel_Queue){
  Rel_Queue_Node_Ptr temp = Rel_Queue->tail;
  Rel_Queue_Node_Ptr new_node = Create_New_Node(rel);
  temp->next = new_node;
  Rel_Queue->tail = new_node;
}

static void Insert_Rel_Node(int rel, Rel_Queue_Ptr Rel_Queue){
  //printf("insert %d\n", rel);
  if(Rel_Queue->head==NULL)
    Insert_In_Empty_Queue(rel, Rel_Queue);
  else
    Insert_At_End(rel, Rel_Queue);
}

static int Already_in_queue(Rel_Queue_Ptr Queue, int rel) {
  Rel_Queue_Node_Ptr pnode = Queue->head;
  while(pnode) {
    if(pnode->rel == rel) return 1;
    pnode = pnode->next;
  }
  return 0;
}

static int Find_best_combination(Rel_Queue_Ptr Queue, int *rels, int num) {
  int min = INT_MAX;
  for(int i = 0; i < num; i++) {
    if(Already_in_queue(Queue, rels[i])) continue;
    if(rels[i] < min) min = rels[i];
  }

  return min;
}

Rel_Queue_Ptr Prepare_Rel_Queue(Parsed_Query_Ptr Parsed_Query){
  HT Best_Tree;
  int best, num = Get_Num_of_Relations(Parsed_Query);
  int *rels = Get_Relations(Parsed_Query);

  Best_Tree.Table = (Rel_Queue_Ptr*)malloc(num * sizeof(Rel_Queue_Ptr));
  for(int i = 0; i < num; i++) {
    Best_Tree.Table[i] = Create_Rel_Queue();
    Insert_Rel_Node(rels[i], Best_Tree.Table[i]);
    printf("%d inserted \n", Best_Tree.Table[i]->head->rel);
    for(int j = 1; j < num - 1; j++) {
	  int best = Find_best_combination(Best_Tree.Table[i], rels, num);
      Insert_Rel_Node(best, Best_Tree.Table[i]);
	}
    Print_Rel_Queue(Best_Tree.Table[i]);
  }
  
  return Best_Tree.Table[0];
}

///////////////////////////////////////////////////////////////////////////////////
static void Compute_Join_Stats(Join_Ptr Joins, int num_of_joins, Table_Ptr Table) {
  Shell_Ptr temp = Get_Table_Array(Table);

  for(int i = 0; i < num_of_joins; i++) {
    Join_Ptr Join = Get_Join_by_index(Joins, i);
    
	if(Is_Self_Join(Join)) continue;

    //get relations and columns
    int rel1 = Get_Relation_1(Join);
    int rel2 = Get_Relation_2(Join);
    Shell_Ptr Shell1 = Get_Shell_by_index(temp, rel1);
    Shell_Ptr Shell2 = Get_Shell_by_index(temp, rel2);
    int col1 = Get_Column_1(Join);
    int col2 = Get_Column_2(Join);

	printf("\n%d.%d = %d.%d\n", rel1, col1, rel2, col2);

    //compute l and u
    uint64_t u = Get_Column_u(Shell2, col2);
    if(Get_Column_u(Shell2, col2) > Get_Column_u(Shell1, col1))
      u = Get_Column_u(Shell1, col1);
    uint64_t l = Get_Column_l(Shell1, col1);
    if(Get_Column_l(Shell2, col2) > Get_Column_l(Shell1, col1))
      l = Get_Column_l(Shell2, col2);

    //compute f and d
    uint64_t fa = Get_Column_f(Shell1, col1);
    uint64_t da = Get_Column_d(Shell1, col1);
    uint64_t fb = Get_Column_f(Shell2, col2);
    uint64_t db = Get_Column_d(Shell2, col2);
   
    uint64_t n = u - l + 1;
//    printf("fa * fb / n -> %llu * %llu / %llu\n", fa, fb, n);
    uint64_t f = fa * fb / n;
//    printf("f -> %llu\n", f);

 //   printf("da * db / n -> %llu * %llu / %llu\n", da, db, n);
    uint64_t d = da * db / n;
 //   printf("d -> %llu\n", d);

//    for(int i =0; i < Get_num_of_columns(Shell1); i++){
//      printf("BEFORE1\n");
//      printf("l = %llu\n", Get_Column_l(Shell1, i));
//      printf("u = %llu\n", Get_Column_u(Shell1, i));
//      printf("f = %llu\n", Get_Column_f(Shell1, i));
//      printf("d = %llu\n", Get_Column_d(Shell1, i));
//    }
//    printf("\n");
//    for(int i =0; i < Get_num_of_columns(Shell2); i++){
//      printf("BEFORE2\n");
//      printf("l = %llu\n", Get_Column_l(Shell2, i));
//      printf("u = %llu\n", Get_Column_u(Shell2, i));
//      printf("f = %llu\n", Get_Column_f(Shell2, i));
//      printf("d = %llu\n", Get_Column_d(Shell2, i));
//    }

    for(int i = 0; i < Get_num_of_columns(Shell1); i++){
      if(i == col1) {
        Set_Column_l(Shell1, col1, l);
        Set_Column_u(Shell1, col1, u);
        Set_Column_f(Shell1, col1, f);
        Set_Column_d(Shell1, col1, d);
	  } else {
        Set_Column_f(Shell1, i, f);
        uint64_t fc = Get_Column_f(Shell1, i);
        uint64_t dc = Get_Column_d(Shell1, i);
		float d_fraction = d / (float)da;
//		printf("dfraction1 = %f\n", d_fraction);
        float p = power((1 - d_fraction), (fc / dc));
//		printf("p1 = %f\n", p);
		uint64_t t = dc * (1 - p);
//		printf("d = %llu\n", t);
        Set_Column_d(Shell1, i, dc * (1 - p));
	  }
	}
    for(int i =0; i < Get_num_of_columns(Shell2); i++){
      if(i == col2) {
        Set_Column_l(Shell2, col2, l);
        Set_Column_u(Shell2, col2, u);
        Set_Column_f(Shell2, col2, f);
        Set_Column_d(Shell2, col2, d);
	  } else {
        Set_Column_f(Shell2, i, f);
        uint64_t fc = Get_Column_f(Shell2, i);
        uint64_t dc = Get_Column_d(Shell2, i);
		float d_fraction = d / (float)db;
//		printf("dfraction2 = %f\n", d_fraction);
        float p = power((1 - d_fraction), (fc / dc));
//		printf("p2 = %f\n", p);
		uint64_t t = dc * (1 - p);
//		printf("d = %llu\n", t);
        Set_Column_d(Shell2, i, dc * (1 - p));
	  }
	}


    for(int i =0; i < Get_num_of_columns(Shell1); i++){
      printf("AFTER1\n");
      printf("l = %llu\n", Get_Column_l(Shell1, i));
      printf("u = %llu\n", Get_Column_u(Shell1, i));
      printf("f = %llu\n", Get_Column_f(Shell1, i));
      printf("d = %llu\n", Get_Column_d(Shell1, i));
    }
    printf("\n");
    for(int i =0; i < Get_num_of_columns(Shell2); i++){
      printf("AFTER2\n");
      printf("l = %llu\n", Get_Column_l(Shell2, i));
      printf("u = %llu\n", Get_Column_u(Shell2, i));
      printf("f = %llu\n", Get_Column_f(Shell2, i));
      printf("d = %llu\n", Get_Column_d(Shell2, i));
    }

  }
}

///////////////////////////////////////		TILL HERE		///////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////

Execution_Queue_Ptr Prepare_Execution_Queue(Parsed_Query_Ptr Parsed_Query, Table_Ptr Table){

  Execution_Queue_Ptr Execution_Queue=Create_Execution_Queue();
  //1.check for self_joins
  int joins_inserted = 0;
  Check_For_Self_joins(Parsed_Query,Execution_Queue,&joins_inserted);

  //2. Compute Join statistics
  Join_Ptr Joins = Get_Joins(Parsed_Query);
  int num_of_joins = Get_Num_of_Joins(Parsed_Query);
  Compute_Join_Stats(Joins, num_of_joins, Table);

  //3.check for joins with  the same column
  Check_For_Same_Column_joins(Parsed_Query, Execution_Queue, &joins_inserted);
  //4.make sure that every consecutive join conects
  Organize_Joins(Parsed_Query,Execution_Queue,&joins_inserted);
  Fill_the_rest(Parsed_Query, Execution_Queue, &joins_inserted);

  return Execution_Queue;
}


