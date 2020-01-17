#include "Preparator.h"
#include "../Execution_Queue/Execution_Queue.h"
#include "../Join_Struct/Join_Struct.h"
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

typedef struct {
  Rel_Queue_Ptr *Table;
  int counter;
}HT;

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
  if(!Rel_Queue) return;
  printf("===REL QUEUE===\n");

  Rel_Queue_Node_Ptr temp = Rel_Queue->head;
  printf("%lu:\n", Rel_Queue->f);
  while (temp!=NULL){
    printf("(%d:)->\n", temp->rel);
	for(int i = 0; i < temp->num_of_columns; i++) {
      printf("\t%llu, %llu, %lu, %lu\n", temp->stats[i].l, temp->stats[i].u, temp->stats[i].f, temp->stats[i].d);
	}
    temp=temp->next;
  }
  printf("\n\n");
}

static int Find_Relative_Value(int *Rels, int original_value, int num_of_rel) {
  for(int i = 0; i < num_of_rel; i++)
    if(Rels[i] == original_value) return i;
}

/////////////////////////////	STATS	///////////////////////////////////////////
static int64_t Compute_Join_Stats(Join_Ptr Join, Table_Ptr Table, \
Rel_Queue_Ptr Queue, int *Rels, int num_of_rel) {
  if(Is_Self_Join(Join)) return -1;
  
  //get relations and columns
  int rel1 = Get_Relation_1(Join);
  int rel2 = Get_Relation_2(Join);
  int col1 = Get_Column_1(Join);
  int col2 = Get_Column_2(Join);

  //printf("%d.%d = %d.%d\n", rel1, col1, rel2, col2);
  //printf("ORIGINAL\n");
  Rel_Queue_Node_Ptr pnode = Queue->tail;

  //make sure they are in the correct order
  if(pnode->rel != rel1) {
	//swap rels and columns
    int temp = rel1;
	rel1 = rel2;
	rel2 = temp;
	temp = col1;
	col1 = col2;
	col2 = temp;
  }
  Shell_Ptr Shell = Get_Table_Array(Table);
  Shell_Ptr Shell_i = Get_Shell_by_index(Shell, rel2);
  //printf("%d = ...\n", pnode->rel);
  
  //compute l and u
  uint64_t u = Get_Column_u(Shell_i, col2);
  if(u > pnode->stats[col1].u)
    u = pnode->stats[col1].u;
  uint64_t l = Get_Column_l(Shell_i, col2);
  if(pnode->stats[col1].l > l)
    l = pnode->stats[col1].l;

  //get f
  int64_t fa = pnode->stats[col1].f;
  int64_t fb = Get_Column_f(Shell_i, col2);
 
  //compute new f
  uint64_t n = u - l + 1;
//  printf("fa * fb / n -> %llu * %llu / %llu\n", fa, fb, n);
  int64_t f = 0;
  if(fa && fb) f = fa * fb / n;
  //printf("f -> %lu\n", f);

  return f;
}

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

static void Update_Join_Stats(Join_Ptr Join, Rel_Queue_Ptr Queue, int *Rels, int num_of_rel) {
  //Print_Rel_Queue(Queue);
  if(Is_Self_Join(Join)) return;
  printf("\nUPDATE\n");

  //get relations and columns
  int rel1 = Get_Relation_1(Join);
  int rel2 = Get_Relation_2(Join);
  int col1 = Get_Column_1(Join);
  int col2 = Get_Column_2(Join);
  printf("%d.%d = %d.%d\n", rel1, col1, rel2, col2);
  //printf("ORIGINAL\n");
  Rel_Queue_Node_Ptr prev, last  = Queue->tail;
 // Find_two_last(Queue, &last, &prev);
  prev = Find_Join_Rel(Queue, rel1, rel2);
  printf("%d = %d\n", prev->rel, last->rel);
  
  //make sure they are in the correct order
  if(prev->rel != rel1) {
	printf("%d and %d are different\nswap\n", prev->rel, rel1);
    Rel_Queue_Node_Ptr temp = prev;
	prev = last;
    last = temp;	
  }

  //compute l and u
  uint64_t u = last->stats[col2].u;
  if(u > prev->stats[col1].u)
    u = prev->stats[col1].u;
  uint64_t l = last->stats[col2].l;
  if(prev->stats[col1].l > l)
    l = prev->stats[col1].l;

  //get f and d
  int64_t fa = prev->stats[col1].f;
  int64_t da = prev->stats[col1].d;
  int64_t fb = last->stats[col2].f;
  int64_t db = last->stats[col2].d;
 
//  for(int i =0; i < prev->num_of_columns; i++){
//    printf("BEFORE1\n");
//    printf("l = %llu\n", prev->stats[i].l);
//    printf("u = %llu\n", prev->stats[i].u);
//    printf("f = %llu\n", prev->stats[i].f);
//    printf("d = %llu\n", prev->stats[i].d);
//  }
//  printf("\n");
//  for(int i =0; i < last->num_of_columns; i++){
//    printf("BEFORE2\n");
//    printf("l = %llu\n", last->stats[i].l);
//    printf("u = %llu\n", last->stats[i].u);
//    printf("f = %llu\n", last->stats[i].f);
//    printf("d = %llu\n", last->stats[i].d);
//  }

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

  for(int i =0; i < prev->num_of_columns; i++){
    if(i == col1) {
      prev->stats[col1].l =  l;
      prev->stats[col1].u =  u;
      prev->stats[col1].d =  d;
    } else {
	  if(f && d) {
        int64_t fc = prev->stats[i].f;
        int64_t dc = prev->stats[i].d;
        float d_fraction = d / (float)da;
        //printf("dfraction1 = %f\n", d_fraction);
        float p = power((1 - d_fraction), (fc / dc));
        //printf("p1 = %f\n", p);
        //uint64_t t = dc * (1 - p);
        //printf("d = %llu\n", t);
        prev->stats[i].d = dc * (1 - p);
	  } else prev->stats[i].d = 0;
    }
    prev->stats[i].f = f;
  }
  for(int i =0; i < last->num_of_columns; i++){
    if(i == col2) {
      last->stats[col2].l =  l;
      last->stats[col2].u =  u;
      last->stats[col2].d =  d;
    } else {
	  if(f && d) {
        int64_t fc = last->stats[i].f;
        int64_t dc = last->stats[i].d;
        float d_fraction = d / (float)db;
        //printf("dfraction2 = %f\n", d_fraction);
        float p = power((1 - d_fraction), (fc / dc));
        //printf("p2 = %f\n", p);
        //uint64_t t = dc * (1 - p);
        //printf("d = %llu\n", t);
        last->stats[i].d = dc * (1 - p);
	  } else last->stats[i].d = 0;
	}
    last->stats[i].f = f;
  }

//  for(int i =0; i < prev->num_of_columns; i++){
//    printf("AFTER1\n");
//    printf("l = %llu\n", prev->stats[i].l);
//    printf("u = %llu\n", prev->stats[i].u);
//    printf("f = %llu\n", prev->stats[i].f);
//    printf("d = %llu\n", prev->stats[i].d);
//  }
//  printf("\n");
//  for(int i =0; i < last->num_of_columns; i++){
//    printf("AFTER2\n");
//    printf("l = %llu\n", last->stats[i].l);
//    printf("u = %llu\n", last->stats[i].u);
//    printf("f = %llu\n", last->stats[i].f);
//    printf("d = %llu\n", last->stats[i].d);
//  }
}
///////////////////////////////////////////////////////////////////////////////////////////////////////

static void Delete_Rel_Queue(Rel_Queue_Ptr Rel_Queue){
  if(Rel_Queue == NULL) return;

  Rel_Queue_Node_Ptr current = Rel_Queue->head;
  Rel_Queue->head=NULL;
  Rel_Queue->tail=NULL;
  while(current!=NULL){
    Rel_Queue_Node_Ptr temp = current;
    current=temp->next;
	free(temp->stats);
    free(temp);
  }
  free(Rel_Queue->Rels_already_passed);
  free(Rel_Queue);
}

static Rel_Queue_Ptr Create_Rel_Queue(int num_of_rel){
  Rel_Queue_Ptr Rel_Queue = (Rel_Queue_Ptr)malloc(sizeof(struct Rel_Queue));
  Rel_Queue->head=NULL;
  Rel_Queue->tail=NULL;
  Rel_Queue->f = INT64_MAX;
  Rel_Queue->Rels_already_passed = (int*)malloc(num_of_rel * sizeof(int));
  for(int i = 0; i < num_of_rel; i++)
    Rel_Queue->Rels_already_passed[i] = 0;
  return Rel_Queue;
}

static Rel_Queue_Node_Ptr Create_New_Node(int rel, Table_Ptr Table){
  Shell_Ptr Shell = Get_Table_Array(Table);
  Shell_Ptr Shell_i = Get_Shell_by_index(Shell, rel);

  Rel_Queue_Node_Ptr new_node = (Rel_Queue_Node_Ptr)malloc(sizeof(struct Rel_Queue_Node));
  new_node->rel = rel;
  int num_of_columns = Get_num_of_columns(Shell_i);
  new_node->num_of_columns = num_of_columns;
  //printf("insert %d, ", rel);
  //printf("with %d columns\n", num_of_columns);
  new_node->stats = (Stats_Ptr)malloc(new_node->num_of_columns * sizeof(struct Stats));
  for(int i = 0; i < new_node->num_of_columns; i++) {
    new_node->stats[i].l = Get_Column_l(Shell_i, i);
    new_node->stats[i].u = Get_Column_u(Shell_i, i);
    new_node->stats[i].f = Get_Column_f(Shell_i, i);
    new_node->stats[i].d = Get_Column_d(Shell_i, i);
  }
  new_node->next = NULL;
  return new_node;
}

static void Insert_In_Empty_Queue(int rel, Table_Ptr Table, Rel_Queue_Ptr Rel_Queue){
  Rel_Queue->head = Create_New_Node(rel, Table);
  Rel_Queue->tail = Rel_Queue->head;
  Rel_Queue->Rels_already_passed[rel] = 1;
}

static void Insert_At_End(int rel, Table_Ptr Table, Rel_Queue_Ptr Rel_Queue){
  Rel_Queue_Node_Ptr temp = Rel_Queue->tail;
  Rel_Queue_Node_Ptr new_node = Create_New_Node(rel, Table);
  temp->next = new_node;
  Rel_Queue->tail = new_node;
  Rel_Queue->Rels_already_passed[rel] = 1;
}

static void Insert_Rel_Node(int rel, Table_Ptr Table, Rel_Queue_Ptr Rel_Queue, int64_t f){
  
  if(Rel_Queue->head==NULL)
	Insert_In_Empty_Queue(rel, Table, Rel_Queue);
  else
    Insert_At_End(rel, Table, Rel_Queue);
  if(f > -1 && Rel_Queue->f == INT64_MAX) {
	//printf("%lu\n", f);
    Rel_Queue->f = f;
  } //else
	//printf("\n");
}

static int Already_in_queue(Rel_Queue_Ptr Queue, int relative) {
  return Queue->Rels_already_passed[relative];
}

static Join_Ptr Find_Join(Join_Ptr Joins, int rel1, int rel2, int num_of_joins) {
  for(int i = 0; i < num_of_joins; i++) {
    Join_Ptr Join = Get_Join_by_index(Joins, i);
    if(rel1 == Get_Relation_1(Join) && rel2 == Get_Relation_2(Join))
      return Join;
    if(rel2 == Get_Relation_1(Join) && rel1 == Get_Relation_2(Join))
      return Join;
  }
  return NULL;
}

static Join_Ptr Connected(int rel1, Rel_Queue_Ptr Queue, Parsed_Query_Ptr Parsed_Query) {
  int num_of_rel = Get_Num_of_Relations(Parsed_Query);
  int *Rels = Get_Relations(Parsed_Query);

  Join_Ptr Joins = Get_Joins(Parsed_Query);
  int num_of_joins = Get_Num_of_Joins(Parsed_Query);

  //find two last rels
  Rel_Queue_Node_Ptr pnode = Queue->head;
  if(pnode->next) {
	while(pnode->next != Queue->tail) {
      pnode = pnode->next;
	}
    Rel_Queue_Node_Ptr prev = pnode;
    Rel_Queue_Node_Ptr last = prev->next;
    //printf("two nodes %d, %d\n", prev->rel, last->rel);
    int rel2 = prev->rel;
    int rel3 = last->rel;
    if(!Find_Join(Joins, rel2, rel3, num_of_joins)) return NULL; 

    Join_Ptr Join = Find_Join(Joins, rel1, rel2, num_of_joins);
    if(Join) return Join;
    Join = Find_Join(Joins, rel1, rel3, num_of_joins);
    if(Join) return Join;
  //only one node
  } else {
    //printf("one node %d\n", pnode->rel);
    int rel2 = pnode->rel;
    Join_Ptr Join = Find_Join(Joins, rel1, rel2, num_of_joins);
    if(Join) return Join; 
  }
  return NULL;
}

static Join_Ptr Find_best_combo(Rel_Queue_Ptr Queue, Parsed_Query_Ptr Parsed_Query, Table_Ptr Table) {
  int num_of_rel = Get_Num_of_Relations(Parsed_Query);
  int *Rels = Get_Relations(Parsed_Query);

  int64_t min = INT64_MAX;
  int best;
  
  Join_Ptr ret;
  int index = 0;
  //check every other relation
  for(int i = 0; i < num_of_rel; i++) {
    if(Already_in_queue(Queue, i)) continue;
    //printf("check %d\n", Rels[i]);

	//if they are connected we find the particular join
	Join_Ptr Join = Connected(i, Queue, Parsed_Query);
	if(Join) {
	  //printf("%d is connected with some rel from queue\n", i);
     
	  //Compute Join stats
      int64_t f = Compute_Join_Stats(Join, Table, Queue, Rels, num_of_rel);
	  //Compare f
      if(f < min) {
        min = f;
		best = Rels[i];
		ret = Join;
		index = i;
	  }
	}
	else continue;
  }
  printf("found %d:%d with f = %lu\n", index, best, min);
  Insert_Rel_Node(index, Table, Queue, min);
  return ret;
}

static Rel_Queue_Ptr Push_last_rel(Rel_Queue_Ptr Queue, Parsed_Query_Ptr Parsed_Query, Table_Ptr Table) {
  if(Queue == NULL) return NULL;
  int num_of_rel = Get_Num_of_Relations(Parsed_Query);
  int *Rels = Get_Relations(Parsed_Query);
  Shell_Ptr Shell = Get_Table_Array(Table);

  for(int i = 0; i < num_of_rel; i++) {
    if(Already_in_queue(Queue, i)) continue;//Rels[i], Rels, num_of_rel)) continue;
	if(Connected(i, Queue, Parsed_Query)) {
      Shell_Ptr Shell_i = Get_Shell_by_index(Shell, i);
	  int num_of_columns = Get_num_of_columns(Shell_i);
	  //printf("last push %d\n", Rels[i]);
      Insert_Rel_Node(i, Table, Queue, -1);
	  return Queue;
    }
  }
  return NULL;
}

static int Exists_better_combo(HT Best_Tree, Rel_Queue_Ptr Current_Queue, int num_of_rel) {
  Rel_Queue_Ptr *Table = Best_Tree.Table;
  for(int i = 0; i < Best_Tree.counter; i++) {
	if(Table[i]) {
    //  printf("check %d\n", Table[i]->head->rel);
      if(Table[i] == Current_Queue) continue;
      if(Table[i]->f <= Current_Queue->f) return 1;
	}
  }
  return 0;
}

static int Choose_Best_Queue(HT Best_Tree) {
  Rel_Queue_Ptr *Table = Best_Tree.Table;
  int64_t min = INT64_MAX;
  int best = 0;
  for(int i = 0; i < Best_Tree.counter; i++) {
    if(Table[i]) {
	  //printf("check for best: \n");
      if(Table[i]->f < min) {
	    //printf("%lu\n", Table[i]->f);
        min = Table[i]->f;
	    best = i;
	  }
	}
  }
  return best;
}

Rel_Queue_Ptr Prepare_Rel_Queue(Parsed_Query_Ptr Parsed_Query, Table_Ptr Table){
  HT Best_Tree;
  Best_Tree.counter = 0;
  int best, num_of_rel = Get_Num_of_Relations(Parsed_Query);
  int *Rels = Get_Relations(Parsed_Query);

  //Shell_Ptr Shell = Get_Table_Array(Table);
  Best_Tree.Table = (Rel_Queue_Ptr*)malloc(num_of_rel * sizeof(Rel_Queue_Ptr));
  //for each relation in query
  for(int i = 0; i < num_of_rel; i++) {
    //Shell_Ptr Shell_i = Get_Shell_by_index(Shell, i);
	//int num_of_columns = Get_num_of_columns(Shell_i);

	int found_better = 0;
    Best_Tree.Table[i] = Create_Rel_Queue(num_of_rel);
    Best_Tree.counter++;
    printf("\n");
    Insert_Rel_Node(i, Table, Best_Tree.Table[i], -1);
    printf("%d inserted\n", Best_Tree.Table[i]->head->rel);

	//find all possible paths
    for(int j = 1; j < num_of_rel - 1; j++) {
	  Join_Ptr Join = Find_best_combo(Best_Tree.Table[i], Parsed_Query, Table);

	  //if we have already found a better combination to start with
	  //delete this one
	  if(Exists_better_combo(Best_Tree, Best_Tree.Table[i], num_of_rel)) {
	    printf("better combo exists\n");
	    found_better = 1;
	    Delete_Rel_Queue(Best_Tree.Table[i]);
        Best_Tree.Table[i] = NULL;
	    break;
	  } else {
		Print_Join(Join);
		printf("\n");
		//printf("passed better combo exists\n");
		//at this point we have to update the join-stats
		//but temporarily since we are not sure
		//we ll follow this path
		//so we keep a struct with the stats for each node
		Update_Join_Stats(Join, Best_Tree.Table[i], Rels, num_of_rel);
	  }
	}
	if(found_better) continue;
	//if we cannot connect the last relation with the queue
	//delete this path
    if(!Push_last_rel(Best_Tree.Table[i], Parsed_Query, Table)) {
	  printf("not possible path\n");
	  Delete_Rel_Queue(Best_Tree.Table[i]);
      Best_Tree.Table[i] = NULL;
	}
  }
  best = Choose_Best_Queue(Best_Tree);
  //printf("BEST -> %d\n", best);
  for(int i = 0; i < Best_Tree.counter; i++) {
    if(i != best) {
      Delete_Rel_Queue(Best_Tree.Table[i]);
      Best_Tree.Table[i] = NULL;
    }
  }
  //Print_Rel_Queue(Best_Tree.Table[best]);
  return Best_Tree.Table[best];
}


static void Fill_Execution_Queue(Parsed_Query_Ptr Parsed_Query,\
Execution_Queue_Ptr Execution_Queue, Rel_Queue_Ptr Rel_Queue) {
  int *Rels = Get_Relations(Parsed_Query);
  int num_of_rel = Get_Num_of_Relations(Parsed_Query);

  //Print_Rel_Queue(Rel_Queue);
  Rel_Queue_Node_Ptr pnode = Rel_Queue->head;
 
  Join_Ptr Joins = Get_Joins(Parsed_Query);
  int num_of_joins = Get_Num_of_Joins(Parsed_Query);

  int prev;
  while(pnode && pnode->next) { 
    int rel1 = pnode->rel;
    int rel2 = pnode->next->rel;
	
    Join_Ptr Join;
	while(1) {
	  //printf("JOIN: %d = %d\n", rel1, rel2);
	  Join = Find_Join(Joins, rel1, rel2, num_of_joins);
	  if(Join) break;
	  rel1 = prev;
	}
    //Print_Join(Join);
	printf("\n");
    Insert_Node(Join, Execution_Queue);

    pnode = pnode->next;
    prev = rel1;
  }
  //printf("END\n");

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
  Rel_Queue_Ptr Rel_Queue = Prepare_Rel_Queue(Parsed_Query, Table);
  Fill_Execution_Queue(Parsed_Query, Execution_Queue, Rel_Queue);
  Print_Queue(Execution_Queue);

//  //2. Compute Join statistics
//  Join_Ptr Joins = Get_Joins(Parsed_Query);
//  int num_of_joins = Get_Num_of_Joins(Parsed_Query);
//  for(int i = 0; i < num_of_joins; i++) {
//    Join_Ptr Join = Get_Join_by_index(Joins, i);
//    Compute_Join_Stats(Join, Table);
//  }
  
  //3.check for joins with  the same column
  //Check_For_Same_Column_joins(Parsed_Query, Execution_Queue, &joins_inserted);
  //4.make sure that every consecutive join conects
  //Organize_Joins(Parsed_Query,Execution_Queue,&joins_inserted);
  //Fill_the_rest(Parsed_Query, Execution_Queue, &joins_inserted);

  return Execution_Queue;
}
