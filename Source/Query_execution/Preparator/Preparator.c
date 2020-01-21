#include "Preparator.h"
#include "../Execution_Queue/Execution_Queue.h"
#include "../Join_Struct/Join_Struct.h"
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

typedef struct {
  //Rel_Queue_Ptr *Table;
  Execution_Queue_Ptr *Table;
  int counter;
}HT;

struct Execution_Queue{
  struct Execution_Queue_Node* head;
  struct Execution_Queue_Node* tail;
};

struct Execution_Queue_Node{
  Join_Ptr Join;
  Stats_Ptr stats1;
  Stats_Ptr stats2;
  uint64_t num_of_columns1;
  uint64_t num_of_columns2;
  struct Execution_Queue_Node* next;
};

typedef struct Execution_Queue_Node* Execution_Queue_Node_Ptr;

struct Rel_Queue{
  struct Rel_Queue_Node* head;
  struct Rel_Queue_Node* tail;
  int64_t f;
  int *Rels_already_passed;
};

struct Rel_Queue_Node{
  int rel;
  int num_of_columns;
  Stats_Ptr stats;
  struct Rel_Queue_Node* next;
};

struct Stats{
  uint64_t l;
  uint64_t u;
  int64_t f;
  int64_t d;
}; 

typedef struct Rel_Queue_Node* Rel_Queue_Node_Ptr;

static void Check_For_Self_joins(Parsed_Query_Ptr Parsed_Query,Execution_Queue_Ptr Execution_Queue,int* joins_inserted){
  Join_Ptr Joins_Array = Get_Joins(Parsed_Query);

  for(int i =0;i<Get_Num_of_Joins(Parsed_Query);i++){
    Join_Ptr Current_Join = Get_Join_by_index(Joins_Array,i);
    if(Is_Self_Join(Current_Join)){
      //Insert_Node(Current_Join,Execution_Queue);
      Insert_Node(Current_Join, Execution_Queue, NULL, NULL, 0, 0);
      (*joins_inserted)++;

    }
  }
}

///////////////////////////////////////////////		NEW		///////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////

//static void Print_Rel_Queue(Rel_Queue_Ptr Rel_Queue){
//  if(!Rel_Queue) return;
//  printf("===REL QUEUE===\n");
//
//  Rel_Queue_Node_Ptr temp = Rel_Queue->head;
//  printf("%lu:\n", Rel_Queue->f);
//  while (temp!=NULL){
//    printf("(%d:)->\n", temp->rel);
//	for(int i = 0; i < temp->num_of_columns; i++) {
//      printf("\t%llu, %llu, %lu, %lu\n", temp->stats[i].l, temp->stats[i].u, temp->stats[i].f, temp->stats[i].d);
//	}
//    temp=temp->next;
//  }
//  printf("\n\n");
//}
//
//static int Find_Relative_Value(int *Rels, int original_value, int num_of_rel) {
//  for(int i = 0; i < num_of_rel; i++)
//    if(Rels[i] == original_value) return i;
//}

/////////////////////////////	STATS	///////////////////////////////////////////
static int64_t Compute_Join_Stats(Execution_Queue_Node_Ptr Pnode) {
  Join_Ptr Join = Pnode->Join;
  if(Is_Self_Join(Join)) return -1;
  
  //get relations and columns
  int rel1 = Get_Relation_1(Join);
  int rel2 = Get_Relation_2(Join);
  int col1 = Get_Column_1(Join);
  int col2 = Get_Column_2(Join);
  printf("%d.%d = %d.%d\n", rel1, col1, rel2, col2);

  //compute l and u
  uint64_t u = Get_Join_stats2_u(Pnode, col2);
  if(u > Get_Join_stats1_u(Pnode, col1))
    u = Get_Join_stats1_u(Pnode, col1);
  uint64_t l = Get_Join_stats2_l(Pnode, col2);
  if(Get_Join_stats1_l(Pnode, col1) > l)
    l = Get_Join_stats1_l(Pnode, col1);

  //get f
  int64_t fa = Get_Join_stats1_f(Pnode, col1);
  int64_t fb = Get_Join_stats2_f(Pnode, col2);

  //compute f
  uint64_t n = u - l + 1;
  //printf("fa * fb / n -> %llu * %llu / %llu\n", fa, fb, n);
  int64_t f = 0;
  if(fa && fb) f = fa * fb / n;

  return f;
}

static void Update_Join_Stats(Execution_Queue_Node_Ptr Pnode) {
  Join_Ptr Join = Pnode->Join;
  if(Is_Self_Join(Join)) return;

  printf("\nUPDATE...\n");

  //get relations and columns
  int rel1 = Get_Relation_1(Join);
  int rel2 = Get_Relation_2(Join);
  int col1 = Get_Column_1(Join);
  int col2 = Get_Column_2(Join);
  printf("%d.%d = %d.%d\n", rel1, col1, rel2, col2);

  //compute l and u
  uint64_t u = Get_Join_stats2_u(Pnode, col2);
  if(u > Get_Join_stats1_u(Pnode, col1))
    u = Get_Join_stats1_u(Pnode, col1);
  uint64_t l = Get_Join_stats2_l(Pnode, col2);
  if(Get_Join_stats1_l(Pnode, col1) > l)
    l = Get_Join_stats1_l(Pnode, col1);

  //get f and d
  int64_t fa = Get_Join_stats1_f(Pnode, col1);
  int64_t da = Get_Join_stats1_d(Pnode, col1);
  int64_t fb = Get_Join_stats2_f(Pnode, col2);
  int64_t db = Get_Join_stats2_d(Pnode, col2);

//  printf("%llu \n", Get_Node_num_of_columns1(Pnode));
//  for(int i =0; i < Get_Node_num_of_columns1(Pnode); i++){
//    printf("BEFORE1\n");
//    printf("l = %llu\n", Get_Join_stats1_l(Pnode, i));
//    printf("u = %llu\n", Get_Join_stats1_u(Pnode, i));
//    printf("f = %llu\n", Get_Join_stats1_f(Pnode, i));
//    printf("d = %llu\n", Get_Join_stats1_d(Pnode, i));
//  }
//  printf("\n");
//  printf("%llu \n", Get_Node_num_of_columns2(Pnode));
//  for(int i =0; i < Get_Node_num_of_columns2(Pnode); i++){
//    printf("BEFORE2\n");
//    printf("l = %llu\n", Get_Join_stats2_l(Pnode, i));
//    printf("u = %llu\n", Get_Join_stats2_u(Pnode, i));
//    printf("f = %llu\n", Get_Join_stats2_f(Pnode, i));
//    printf("d = %llu\n", Get_Join_stats2_d(Pnode, i));
//  }
//  printf("\n");

  //compute f and d
  uint64_t n = u - l + 1;
  //printf("fa * fb / n -> %llu * %llu / %llu\n", fa, fb, n);
  int64_t f = 0;
  if(fa && fb) f = fa * fb / n;
  //printf("f -> %lu\n", f);
  //printf("da * db / n -> %llu * %llu / %llu\n", da, db, n);
  int64_t d = 0;
  if(da && db) d = da * db / n;
  //printf("d -> %llu\n", d);

  for(int i = 0; i < Get_Node_num_of_columns1(Pnode); i++){
    if(i == col1) {
      Set_Join_stats1_l(Pnode, col1, l);
      Set_Join_stats1_u(Pnode, col1, u);
      Set_Join_stats1_d(Pnode, col1, d);
    } else {
	  if(f && d) {
        int64_t fc = Get_Join_stats1_f(Pnode, i);
        int64_t dc = Get_Join_stats1_d(Pnode, i);
        float d_fraction = d / (float)da;
        //printf("dfraction1 = %f\n", d_fraction);
        float p = power((1 - d_fraction), (fc / dc));
        //printf("p1 = %f\n", p);
        uint64_t t = dc * (1 - p);
        //printf("d = %llu\n", t);
        Set_Join_stats1_d(Pnode, i, t);
	  } else Set_Join_stats1_d(Pnode, i, 0);
    }
    Set_Join_stats1_f(Pnode, i, f);
  }
  printf("\n");
  for(int i =0; i < Get_Node_num_of_columns2(Pnode); i++){
    if(i == col2) {
      Set_Join_stats2_l(Pnode, col2, l);
      Set_Join_stats2_u(Pnode, col2, u);
      Set_Join_stats2_d(Pnode, col2, d);
    } else {
	  if(f && d) {
        int64_t fc = Get_Join_stats2_f(Pnode, i);
        int64_t dc = Get_Join_stats2_d(Pnode, i);
        float d_fraction = d / (float)db;
        //printf("dfraction2 = %f\n", d_fraction);
        float p = power((1 - d_fraction), (fc / dc));
        //printf("p2 = %f\n", p);
        //uint64_t t = dc * (1 - p);
        //printf("d = %llu\n", t);
        Set_Join_stats2_d(Pnode, i, dc * (1 - p));
	  } else Set_Join_stats2_d(Pnode, i, 0);
	}
    Set_Join_stats2_f(Pnode, i, f);
  }

  //printf("u = %llu, l = %llu\n", u, l);
  //printf("f = %llu, d = %llu\n", f, d);

//  for(int i =0; i < Get_Node_num_of_columns1(Pnode); i++){
//    printf("AFTER1\n");
//    printf("l = %llu\n", Get_Join_stats1_l(Pnode, i));
//    printf("u = %llu\n", Get_Join_stats1_u(Pnode, i));
//    printf("f = %llu\n", Get_Join_stats1_f(Pnode, i));
//    printf("d = %llu\n", Get_Join_stats1_d(Pnode, i));
//  }
//  printf("\n");
//  for(int i =0; i < Get_Node_num_of_columns2(Pnode); i++){
//    printf("AFTER2\n");
//    printf("l = %llu\n", Get_Join_stats2_l(Pnode, i));
//    printf("u = %llu\n", Get_Join_stats2_u(Pnode, i));
//    printf("f = %llu\n", Get_Join_stats2_f(Pnode, i));
//    printf("d = %llu\n", Get_Join_stats2_d(Pnode, i));
//  }
}
///////////////////////////////////////////////////////////////////////////////////////////////////////

//static void Delete_Rel_Queue(Rel_Queue_Ptr Rel_Queue){
//  if(Rel_Queue == NULL) return;
//
//  Rel_Queue_Node_Ptr current = Rel_Queue->head;
//  Rel_Queue->head=NULL;
//  Rel_Queue->tail=NULL;
//  while(current!=NULL){
//    Rel_Queue_Node_Ptr temp = current;
//    current=temp->next;
//	free(temp->stats);
//    free(temp);
//  }
//  free(Rel_Queue->Rels_already_passed);
//  free(Rel_Queue);
//}
//
//static Rel_Queue_Ptr Create_Rel_Queue(int num_of_rel){
//  Rel_Queue_Ptr Rel_Queue = (Rel_Queue_Ptr)malloc(sizeof(struct Rel_Queue));
//  Rel_Queue->head=NULL;
//  Rel_Queue->tail=NULL;
//  Rel_Queue->f = INT64_MAX;
//  Rel_Queue->Rels_already_passed = (int*)malloc(num_of_rel * sizeof(int));
//  for(int i = 0; i < num_of_rel; i++)
//    Rel_Queue->Rels_already_passed[i] = 0;
//  return Rel_Queue;
//}
//
//static Rel_Queue_Node_Ptr Create_New_Node(int rel, Table_Ptr Table){
//  Shell_Ptr Shell = Get_Table_Array(Table);
//  Shell_Ptr Shell_i = Get_Shell_by_index(Shell, rel);
//
//  Rel_Queue_Node_Ptr new_node = (Rel_Queue_Node_Ptr)malloc(sizeof(struct Rel_Queue_Node));
//  new_node->rel = rel;
//  int num_of_columns = Get_num_of_columns(Shell_i);
//  new_node->num_of_columns = num_of_columns;
//  //printf("insert %d, ", rel);
//  //printf("with %d columns\n", num_of_columns);
//  new_node->stats = (Stats_Ptr)malloc(new_node->num_of_columns * sizeof(struct Stats));
//  for(int i = 0; i < new_node->num_of_columns; i++) {
//    new_node->stats[i].l = Get_Column_l(Shell_i, i);
//    new_node->stats[i].u = Get_Column_u(Shell_i, i);
//    new_node->stats[i].f = Get_Column_f(Shell_i, i);
//    new_node->stats[i].d = Get_Column_d(Shell_i, i);
//  }
//  new_node->next = NULL;
//  return new_node;
//}
//
//static void Insert_In_Empty_Queue(int rel, Table_Ptr Table, Rel_Queue_Ptr Rel_Queue){
//  Rel_Queue->head = Create_New_Node(rel, Table);
//  Rel_Queue->tail = Rel_Queue->head;
//  Rel_Queue->Rels_already_passed[rel] = 1;
//}
//
//static void Insert_At_End(int rel, Table_Ptr Table, Rel_Queue_Ptr Rel_Queue){
//  Rel_Queue_Node_Ptr temp = Rel_Queue->tail;
//  Rel_Queue_Node_Ptr new_node = Create_New_Node(rel, Table);
//  temp->next = new_node;
//  Rel_Queue->tail = new_node;
//  Rel_Queue->Rels_already_passed[rel] = 1;
//}
//
//static void Insert_Rel_Node(int rel, Table_Ptr Table, Rel_Queue_Ptr Rel_Queue, int64_t f){
//  
//  if(Rel_Queue->head==NULL)
//	Insert_In_Empty_Queue(rel, Table, Rel_Queue);
//  else
//    Insert_At_End(rel, Table, Rel_Queue);
//  if(f > -1 && Rel_Queue->f == INT64_MAX) {
//	//printf("%lu\n", f);
//    Rel_Queue->f = f;
//  } //else
//	//printf("\n");
//}
//

//static Rel_Queue_Ptr Push_last_rel(Rel_Queue_Ptr Queue, Parsed_Query_Ptr Parsed_Query, Table_Ptr Table) {
//  if(Queue == NULL) return NULL;
//  int num_of_rel = Get_Num_of_Relations(Parsed_Query);
//  int *Rels = Get_Relations(Parsed_Query);
//  Shell_Ptr Shell = Get_Table_Array(Table);
//
//  for(int i = 0; i < num_of_rel; i++) {
//    if(Already_in_queue(Queue, i)) continue;//Rels[i], Rels, num_of_rel)) continue;
//	if(Connected(i, Queue, Parsed_Query)) {
//      Shell_Ptr Shell_i = Get_Shell_by_index(Shell, i);
//	  int num_of_columns = Get_num_of_columns(Shell_i);
//	  //printf("last push %d\n", Rels[i]);
//      Insert_Rel_Node(i, Table, Queue, -1);
//	  return Queue;
//    }
//  }
//  return NULL;
//}
//
//static int Exists_better_combo(HT Best_Tree, Rel_Queue_Ptr Current_Queue, int num_of_rel) {
//  Rel_Queue_Ptr *Table = Best_Tree.Table;
//  for(int i = 0; i < Best_Tree.counter; i++) {
//	if(Table[i]) {
//    //  printf("check %d\n", Table[i]->head->rel);
//      if(Table[i] == Current_Queue) continue;
//      if(Table[i]->f <= Current_Queue->f) return 1;
//	}
//  }
//  return 0;
//}
//
//static int Choose_Best_Queue(HT Best_Tree) {
//  Rel_Queue_Ptr *Table = Best_Tree.Table;
//  int64_t min = INT64_MAX;
//  int best = 0;
//  for(int i = 0; i < Best_Tree.counter; i++) {
//    if(Table[i]) {
//	  //printf("check for best: \n");
//      if(Table[i]->f < min) {
//	    //printf("%lu\n", Table[i]->f);
//        min = Table[i]->f;
//	    best = i;
//	  }
//	}
//  }
//  return best;
//}

static Rel_Queue_Node_Ptr Find_Join_Rel(Rel_Queue_Ptr Queue, int rel1, int rel2) {
  Rel_Queue_Node_Ptr pnode = Queue->head ;
  while(pnode && pnode->rel != rel1 && pnode->rel != rel2) {
  //while(pnode->next != Queue->tail) {
    pnode = pnode->next;
  }
  return pnode;
  //*prev = pnode;
  //*last = Queue->tail;
}

static int Already_in_queue(Execution_Queue_Ptr Queue, Join_Ptr Join) {
//	printf("check\t");
//	Print_Join(Join);
//	printf("\n");
  Execution_Queue_Node_Ptr pnode = Queue->head;
  while(pnode) {
    if(Is_the_Same_Join(Join, pnode->Join))
      return 1;
    pnode = pnode->next;
  }

  return 0;
}

//static Join_Ptr Find_best_combo(HT Tree, Execution_Queue_Node_Ptr Queue, Execution_Queue_Node_Ptr Pnode) {
static int Find_best_combo(HT Tree, Execution_Queue_Node_Ptr Queue, Execution_Queue_Node_Ptr Pnode) {
  Join_Ptr Current = Pnode->Join;
  printf("\n\n========\n------>");
  Print_Join(Current);
  printf("\n");

  int64_t min = INT64_MAX;
  Join_Ptr best = NULL;
  int index = 0;

  //check every other join
  for(int i = 0; i < Tree.counter; i++) {
    Join_Ptr Join_i = Tree.Table[i]->head->Join;
	//printf("check %d:\t", i);
	//Print_Join(Join_i);
	//printf("\n");
    
	if(Already_in_queue(Queue, Join_i)) {
	  //printf("nop\n");
	  continue;
	}

	//if they are connected we find the particular join
    if(Check_if_joins_Connect(Current, Join_i)) {
	  //printf("Join #%d is connected with some rel from queue\n", i);
     
	  //Compute Join stats
      //int64_t f = Compute_Join_Stats(Tree.Table[i]->head);

	  //Change relation's stats
      int64_t f = Get_Join_stats1_f(Tree.Table[i]->head, 0);
	  Print_Join(Tree.Table[i]->head->Join);
      printf("with f %lu\n", f);
	  //Compare f
      if(f < min) {
        min = f;
	    best = Join_i;
	    index = i;
	  }
	}
	else continue;
  }
  if(best) {
    printf("found %d:   ", index);
    Print_Join(best);
    printf(" with f = %lu\n", min);
    //Insert_Node(index, Table, Queue, min);

	Execution_Queue_Node_Ptr best_node = Tree.Table[index]->head;
	Stats_Ptr stats1 = Get_Node_stats1(best_node);
	Stats_Ptr stats2 = Get_Node_stats2(best_node);
	uint64_t num_of_columns1 = Get_Node_num_of_columns1(best_node);
	uint64_t num_of_columns2 = Get_Node_num_of_columns2(best_node);

    Insert_Node(best, Queue, stats1, stats2, num_of_columns1, num_of_columns2);
	return 1;
  } else {
	printf("not possible\n");
	return 0;
  }
}

static void Get_Join_original_rel(Shell_Ptr *Shell1, Shell_Ptr *Shell2, Join_Ptr Join, Shell_Ptr Shell, int *Rels) {
  int rel1 = Get_Relation_1(Join);
  //printf("rel1: %d\n",rel1);
  int rel2 = Get_Relation_2(Join);
  //printf("rel2: %d\n",rel2);
  
  *Shell1 = Get_Shell_by_index(Shell, rel1);
  *Shell2 = Get_Shell_by_index(Shell, rel2);
}

static Execution_Queue_Ptr Prepare_Queue(Parsed_Query_Ptr Parsed_Query, Table_Ptr Table){
  int num_of_rel = Get_Num_of_Relations(Parsed_Query);
  int *Rels = Get_Relations(Parsed_Query);
  Join_Ptr Joins = Get_Joins(Parsed_Query);
  int num_of_joins = Get_Num_of_Joins(Parsed_Query);

  Shell_Ptr Shell = Get_Table_Array(Table);

  HT Best_Tree;
  Best_Tree.counter = 0;
  Best_Tree.Table = (Execution_Queue_Ptr*)malloc(num_of_joins * sizeof(Execution_Queue_Ptr));

  int64_t best_f = INT64_MAX;
  int best_queue = 0;
  for(int i = 0; i < num_of_joins; i++) {
    Best_Tree.Table[i] = Create_Execution_Queue();
    Best_Tree.counter++;

    printf("\n");
    Join_Ptr Current = Get_Join_by_index(Joins, i);

	//get stats
    Shell_Ptr Shell1, Shell2;
    Get_Join_original_rel(&Shell1, &Shell2, Current, Shell, Rels);

	Stats_Ptr stats1 = Get_Shell_stats(Shell1);
	uint64_t num_of_columns1 = Get_num_of_columns(Shell1);
	Stats_Ptr stats2 = Get_Shell_stats(Shell2);
	uint64_t num_of_columns2 = Get_num_of_columns(Shell2);

    //Insert_Node(Current, Best_Tree.Table[i]);
    Insert_Node(Current, Best_Tree.Table[i], stats1, stats2, num_of_columns1, num_of_columns2);
    //Insert_Rel_Node(i, Table, Best_Tree.Table[i], -1);
	Update_Join_Stats(Best_Tree.Table[i]->head);

	Print_Join(Best_Tree.Table[i]->head->Join);
	int64_t f = Get_Join_stats1_f(Best_Tree.Table[i]->head, 0);
    printf("\tinserted ");
    printf("with f = %lu\n", f);
  }
 
  int found_better = 0;
  for(int i = 0; i < Best_Tree.counter; i++) {
    int not_possible_path = 0;
	if(Best_Tree.counter == 1) {
      best_f = Get_Join_stats1_f(Best_Tree.Table[i]->head, 0);
      best_queue = i;
	  break;
	}
	//find all possible paths
    for(int j = 1; j < Best_Tree.counter; j++) {
      Execution_Queue_Node_Ptr Current_Node = Best_Tree.Table[i]->tail;
	  //Join_Ptr Join = Find_best_combo(Best_Tree.Table[i], Parsed_Query, Table);
      if(!Find_best_combo(Best_Tree, Best_Tree.Table[i], Current_Node)) {
		not_possible_path = 1;
	    break;
	  }
	}
	if(not_possible_path) continue;
    //Print_Queue(Best_Tree.Table[i]);
	int64_t f = Get_Join_stats1_f(Best_Tree.Table[i]->head, 0);
	if(f < best_f) {
      best_f = f;
      best_queue = i;
	}

	  //if we have already found a better combination to start with
	  //delete this one
	  //if(Exists_better_combo(Best_Tree, Best_Tree.Table[i], num_of_rel)) {
	  //  printf("better combo exists\n");
	  //  found_better = 1;
	  //  Delete_Rel_Queue(Best_Tree.Table[i]);
      //  Best_Tree.Table[i] = NULL;
	  //  break;
	  //} else {
	  //  Print_Join(Join);
	  //  printf("\n");
	  //  //printf("passed better combo exists\n");
	  //  //at this point we have to update the join-stats
	  //  //but temporarily since we are not sure
	  //  //we ll follow this path
	  //  //so we keep a struct with the stats for each node
	  //  Update_Join_Stats(Join, Best_Tree.Table[i], Rels, num_of_rel);
	  //}
	//if(found_better) continue;
  }
  printf("BEST EX QUEUE is #%d with f = %lu\n", best_queue, best_f);
  Print_Queue(Best_Tree.Table[best_queue]);

}




//static void Fill_Execution_Queue(Parsed_Query_Ptr Parsed_Query,\
//Execution_Queue_Ptr Execution_Queue, Rel_Queue_Ptr Rel_Queue) {
//  int *Rels = Get_Relations(Parsed_Query);
//  int num_of_rel = Get_Num_of_Relations(Parsed_Query);
//
//  //Print_Rel_Queue(Rel_Queue);
//  Rel_Queue_Node_Ptr pnode = Rel_Queue->head;
// 
//  Join_Ptr Joins = Get_Joins(Parsed_Query);
//  int num_of_joins = Get_Num_of_Joins(Parsed_Query);
//
//  int prev;
//  while(pnode && pnode->next) { 
//    int rel1 = pnode->rel;
//    int rel2 = pnode->next->rel;
//	
//    Join_Ptr Join;
//	while(1) {
//	  //printf("JOIN: %d = %d\n", rel1, rel2);
//	  Join = Find_Join(Joins, rel1, rel2, num_of_joins);
//	  if(Join) break;
//	  rel1 = prev;
//	}
//    //Print_Join(Join);
//	printf("\n");
//    Insert_Node(Join, Execution_Queue);
//
//    pnode = pnode->next;
//    prev = rel1;
//  }
//  //printf("END\n");
//
//}

///////////////////////////////////////		TILL HERE		///////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////

Execution_Queue_Ptr Prepare_Execution_Queue(Parsed_Query_Ptr Parsed_Query, Table_Ptr Table){
  int j = Get_Num_of_Joins(Parsed_Query);
  int r = Get_Num_of_Relations(Parsed_Query);

  Execution_Queue_Ptr Execution_Queue=Create_Execution_Queue();

  //1.check for self_joins
  int joins_inserted = 0;
  Check_For_Self_joins(Parsed_Query,Execution_Queue,&joins_inserted);
  //Print_Queue(Execution_Queue);

  //2.Optimizer
  //Rel_Queue_Ptr Rel_Queue = Prepare_Rel_Queue(Parsed_Query, Table);
  Prepare_Queue(Parsed_Query, Table);
  //Fill_Execution_Queue(Parsed_Query, Execution_Queue, Rel_Queue);
  //Print_Queue(Execution_Queue);

  return Execution_Queue;
}
