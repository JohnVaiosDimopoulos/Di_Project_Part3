#include "Execution_Queue.h"
#include <stdlib.h>
#include <stdio.h>

struct Execution_Queue{
  struct Execution_Queue_Node* head;
  struct Execution_Queue_Node* tail;
};

struct Execution_Queue_Node{
  Join_Ptr Join;
  struct Execution_Queue_Node* next;
};

typedef struct Execution_Queue_Node* Execution_Queue_Node_Ptr;

int Is_Empty(Execution_Queue_Ptr Execution_Queue){
  return Execution_Queue->head==NULL;
}

static void Delete_Node(Execution_Queue_Node_Ptr Node){
  Node->Join=NULL;
  Node->next=NULL;
  free(Node);
}

void Delete_Queue(Execution_Queue_Ptr Execution_Queue){
  Execution_Queue_Node_Ptr current= Execution_Queue->head;
  Execution_Queue->head=NULL;
  Execution_Queue->tail=NULL;
  while (current!=NULL){
    Execution_Queue_Node_Ptr temp = current;
    current=temp->next;
    free(temp);
  }
  free(Execution_Queue);
}

Execution_Queue_Ptr Create_Execution_Queue(){
  Execution_Queue_Ptr Execution_Queue = (Execution_Queue_Ptr)malloc(sizeof( struct Execution_Queue));
  Execution_Queue->head=NULL;
  Execution_Queue->tail=NULL;
  return Execution_Queue;
}

Execution_Queue_Node_Ptr Create_New_Node(Join_Ptr Join){
  Execution_Queue_Node_Ptr new_node = (Execution_Queue_Node_Ptr)malloc(sizeof(struct Execution_Queue_Node));
  new_node->Join=Join;
  new_node->next=NULL;
  return new_node;

}

static void Insert_In_Empty_Queue(Join_Ptr Join, Execution_Queue_Ptr Execution_Queue){
  Execution_Queue->head=Create_New_Node(Join);
  Execution_Queue->tail=Execution_Queue->head;
}

static void Insert_At_End(Join_Ptr Join,Execution_Queue_Ptr Execution_Queue){
  Execution_Queue_Node_Ptr temp = Execution_Queue->tail;
  Execution_Queue_Node_Ptr new_node = Create_New_Node(Join);
  temp->next=new_node;
  Execution_Queue->tail=new_node;
}

void Insert_Node(Join_Ptr Join, Execution_Queue_Ptr Execution_Queue){
  if(Execution_Queue->head==NULL)
    Insert_In_Empty_Queue(Join,Execution_Queue);
  else
    Insert_At_End(Join,Execution_Queue);
}

Join_Ptr Pop_Next_join(Execution_Queue_Ptr Execution_Queue){
  if(Is_Empty(Execution_Queue))
    return NULL;
  Execution_Queue_Node_Ptr temp = Execution_Queue->head;
  Execution_Queue->head = temp->next;
  if(Execution_Queue->head==NULL)
    Execution_Queue->tail=NULL;
  Join_Ptr Join = temp->Join;
  Delete_Node(temp);
  return Join;
}

int Is_in_Queue(Join_Ptr Join_tested,Execution_Queue_Ptr Execution_Queue){
  Execution_Queue_Node_Ptr temp=Execution_Queue->head;
  while(temp!=NULL){
    if(Is_the_Same_Join(temp->Join,Join_tested))
      return 1;
    temp=temp->next;
  }
  return 0;
}

int Connects_with_last_join(Join_Ptr Join,Execution_Queue_Ptr Execution_Queue){
  if(Check_if_joins_Connect(Join,Execution_Queue->tail->Join))
    return 1;
  return 0;
}

void Print_Queue(Execution_Queue_Ptr Execution_Queue){
  printf("===EXECUTION QUEUE===\n");
  Execution_Queue_Node_Ptr temp = Execution_Queue->head;
  while (temp!=NULL){
    printf("(");
    Print_Join(temp->Join);
    printf(")->");
    temp=temp->next;
  }
  printf("\n\n");
}





