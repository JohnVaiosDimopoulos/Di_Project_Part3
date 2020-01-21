#include "Preparator.h"
#include "../Execution_Queue/Execution_Queue.h"
#include "../Join_Struct/Join_Struct.h"
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

typedef struct {
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
      Insert_Node(Current_Join, Execution_Queue, NULL, NULL, 0, 0);
    }
  }
}

///////////////////////////////////////////////		NEW		///////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////

//static int Find_which_rel_connects_the_second_with_the_first(Join_Ptr Join1, Join_Ptr Join2) {
//  int rel1 = Get_Relation_1(Join2);
//  int rel2 = Get_Relation_2(Join2);
//
//  if(rel1 == Get_Relation_1(Join1) || rel1 == Get_Relation_2(Join1)) return 1;
//  if(rel2 == Get_Relation_1(Join1) || rel2 == Get_Relation_2(Join1)) return 2;
//
//}

/////////////////////////////	STATS	///////////////////////////////////////////
static int64_t Compute_Join_Stats(Execution_Queue_Node_Ptr Pnode) {
//static Execution_Queue_Node_Ptr Compute_Join_Stats(Execution_Queue_Node_Ptr Pnode1, Execution_Queue_Node_Ptr Pnode2) {
//  printf("COMPUTE...\nJoin1\n");
//  Print_Join(Pnode1->Join);
//  printf("\nJoin2\n");
//  Print_Join(Pnode2->Join);
//  printf("\n");
//  
//  Join_Ptr Join1 = Pnode1->Join;
//  Join_Ptr Join2 = Pnode2->Join;
//  int rel1 = Find_which_rel_connects_the_second_with_the_first(Join2, Join1);
//  int rel2 = Find_which_rel_connects_the_second_with_the_first(Join1, Join2);
//  //printf("the %dth rel\n", rel);
//
//  //Find the new stats
//  Stats_Ptr stats1, stats2;
//  uint64_t num_of_columns1, num_of_columns2;
//  if(rel1 == 2 && rel2 == 1) {
//    printf("2-1\n");
//	 stats1 = Get_Node_stats2(Pnode1);
//	 num_of_columns1 = Get_Node_num_of_columns2(Pnode1);
//	 stats2 = Get_Node_stats2(Pnode2);
//	 num_of_columns2 = Get_Node_num_of_columns2(Pnode2);
//  } else if(rel1 == 1 && rel2 == 1) {
//    printf("1-1\n");
//	 stats1 = Get_Node_stats1(Pnode1);
//	 num_of_columns1 = Get_Node_num_of_columns1(Pnode1);
//	 stats2 = Get_Node_stats2(Pnode2);
//	 num_of_columns2 = Get_Node_num_of_columns2(Pnode2);
//
//  } else if(rel1 == 2 && rel2 == 2) {
//    printf("2-2\n");
//	stats1 = Get_Node_stats1(Pnode2);
//	num_of_columns1 = Get_Node_num_of_columns1(Pnode2);
//	stats2 = Get_Node_stats2(Pnode1);
//	num_of_columns2 = Get_Node_num_of_columns2(Pnode1);
//
//  } else if(rel1 == 1 && rel2 == 2) {
//    printf("1-2\n");
//	stats1 = Get_Node_stats1(Pnode2);
//	num_of_columns1 = Get_Node_num_of_columns1(Pnode2);
//	stats2 = Get_Node_stats1(Pnode1);
//	num_of_columns2 = Get_Node_num_of_columns1(Pnode1);
//  }
//  Execution_Queue_Node_Ptr New = Create_New_Node(Join2, stats1, stats2, num_of_columns1, num_of_columns2);
//  if(New == NULL) printf("NULL\n");
//
//  printf("new \n");
  printf("COMPUTE...\n");
  Join_Ptr Join = Pnode->Join;
  //Print_Join(Join);
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

static int Find_best_combo(HT Tree, Execution_Queue_Node_Ptr Queue, Execution_Queue_Node_Ptr Pnode) {
  Join_Ptr Current = Pnode->Join;
  //printf("\n\n========\n------>");
  //Print_Join(Current);
  printf("\n");

  int64_t min = INT64_MAX;
  Join_Ptr best = NULL;
  int index = 0;

  //check every other join
  for(int i = 0; i < Tree.counter; i++) {
	
	if(Tree.Table[i]->head == NULL) continue;
    Join_Ptr Join_i = Tree.Table[i]->head->Join;
    
	if(Already_in_queue(Queue, Join_i)) {
	  //printf("nop\n");
	  continue;
	}

	//if they are connected we find the particular join
    if(Check_if_joins_Connect(Current, Join_i)) {
	  //printf("Join #%d is connected with some rel from queue\n", i);
     
	  //Compute Join stats
	  //Change relation's stats
      //Compute_Join_Stats(Pnode, Tree.Table[i]->head);

      int64_t f = Get_Join_stats1_f(Tree.Table[i]->head, 0);
	  //Print_Join(Tree.Table[i]->head->Join);
      //printf("with f %lu\n", f);
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
    //printf("found %d:   ", index);
    //Print_Join(best);
    //printf(" with f = %lu\n", min);

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
  int i = 0;
  while(i < num_of_joins) {
    Join_Ptr Current = Get_Join_by_index(Joins, i);

    if(Is_Self_Join(Current)) {
	  i++;
	  continue;
	}
    Best_Tree.Table[Best_Tree.counter] = Create_Execution_Queue();

	//get stats
    Shell_Ptr Shell1, Shell2;
    Get_Join_original_rel(&Shell1, &Shell2, Current, Shell, Rels);

	Stats_Ptr stats1 = Get_Shell_stats(Shell1);
	uint64_t num_of_columns1 = Get_num_of_columns(Shell1);
	Stats_Ptr stats2 = Get_Shell_stats(Shell2);
	uint64_t num_of_columns2 = Get_num_of_columns(Shell2);

    Insert_Node(Current, Best_Tree.Table[Best_Tree.counter], stats1, stats2, num_of_columns1, num_of_columns2);
	Update_Join_Stats(Best_Tree.Table[Best_Tree.counter]->head);

	Print_Join(Best_Tree.Table[Best_Tree.counter]->head->Join);
	int64_t f = Get_Join_stats1_f(Best_Tree.Table[Best_Tree.counter]->head, 0);
    printf("\tinserted ");
    printf("with f = %lu\n", f);
    Best_Tree.counter++;
	i++;
  }
 
  int counter =  Best_Tree.counter;
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
  }
  printf("BEST EX QUEUE is #%d with f = %lu\n", best_queue, best_f);
  //Print_Queue(Best_Tree.Table[best_queue]);
  return(Best_Tree.Table[best_queue]);
}


static void Merge_Execution_Queues(Execution_Queue_Ptr Queue1, Execution_Queue_Ptr Queue2) {
  if(Queue1->tail) {
    Queue1->tail->next = Queue2->head;
  } else {
    Queue1->head = Queue2->head;
  }
  Queue1->tail = Queue2->tail;
}

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
  Execution_Queue_Ptr Queue = Prepare_Queue(Parsed_Query, Table);
  Print_Queue(Queue);
  Merge_Execution_Queues(Execution_Queue, Queue);
  Print_Queue(Execution_Queue);

  return Execution_Queue;
}
