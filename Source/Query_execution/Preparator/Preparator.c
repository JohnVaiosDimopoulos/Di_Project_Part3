#include "Preparator.h"
#include "../Execution_Queue/Execution_Queue.h"
#include "../Join_Struct/Join_Struct.h"
#include <stdio.h>
#include <stdlib.h>

struct Rel_Queue{
  struct Rel_Queue_Node* head;
  struct Rel_Queue_Node* tail;
};

struct Rel_Queue_Node{
  int rel;
  struct Rel_Queue_Node* next;
};

typedef struct HT {
  Rel_Queue_Ptr *Table;
}HT;

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

Rel_Queue_Ptr Prepare_Rel_Queue(Parsed_Query_Ptr Parsed_Query){
  HT Best_Tree;
  int best, num = Get_Num_of_Relations(Parsed_Query);
  int *rels = Get_Relations(Parsed_Query);

  Best_Tree.Table = (Rel_Queue_Ptr*)malloc(num * sizeof(Rel_Queue_Ptr));
  for(int i = 0; i < num; i++) {
    Best_Tree.Table[i] = Create_Rel_Queue();
    Insert_Rel_Node(rels[i], Best_Tree.Table[i]);
    printf("%d inserted \n", Best_Tree.Table[i]->head->rel);
  }
  printf("end\n");
  
 // for(int i = 0; i < num; i++) {
 //   Print_Rel_Queue(Best_Tree.Table[i]);
 // }
  return Best_Tree.Table[0];
}

///////////////////////////////////////		TILL HERE		///////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////

Execution_Queue_Ptr Prepare_Execution_Queue(Parsed_Query_Ptr Parsed_Query){

  Execution_Queue_Ptr Execution_Queue=Create_Execution_Queue();
  //1.check for self_joins
  int joins_inserted = 0;
  Check_For_Self_joins(Parsed_Query,Execution_Queue,&joins_inserted);
  //2.check for joins with  the same column
  Check_For_Same_Column_joins(Parsed_Query,Execution_Queue,&joins_inserted);
  //3.make sure that every consecutive join conects
  Organize_Joins(Parsed_Query,Execution_Queue,&joins_inserted);
  Fill_the_rest(Parsed_Query,Execution_Queue,&joins_inserted);

  return Execution_Queue;
}


