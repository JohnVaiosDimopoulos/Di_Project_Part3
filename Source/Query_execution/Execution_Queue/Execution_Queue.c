#include "Execution_Queue.h"
#include <stdlib.h>
#include <stdio.h>

struct Execution_Queue{
  struct Execution_Queue_Node* head;
  struct Execution_Queue_Node* tail;
};

struct Stats{
  uint64_t l;
  uint64_t u;
  int64_t f;
  int64_t d;
}; 


struct Execution_Queue_Node{
  Join_Ptr Join;
  Stats_Ptr stats1;
  Stats_Ptr stats2;
  uint64_t num_of_columns1;
  uint64_t num_of_columns2;
  struct Execution_Queue_Node* next;
};


int Is_Empty(Execution_Queue_Ptr Execution_Queue){
  return Execution_Queue->head==NULL;
}

static void Delete_Node(Execution_Queue_Node_Ptr Node){
  Node->Join=NULL;
  Node->next=NULL;
  free(Node->stats1);
  free(Node->stats2);
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

//Execution_Queue_Node_Ptr Create_New_Node(Join_Ptr Join) {
Execution_Queue_Node_Ptr Create_New_Node(Join_Ptr Join, \
Stats_Ptr stats1, Stats_Ptr stats2, uint64_t num_of_columns1, uint64_t num_of_columns2){
  Execution_Queue_Node_Ptr new_node = (Execution_Queue_Node_Ptr)malloc(sizeof(struct Execution_Queue_Node));
  new_node->Join=Join;
  //Print_Join(new_node->Join);
  new_node->stats1 = (Stats_Ptr)malloc(num_of_columns1 * sizeof(struct Stats));
  for(int i = 0; i < num_of_columns1; i++) {
    new_node->stats1[i].l = stats1[i].l;
    new_node->stats1[i].u = stats1[i].u;
    new_node->stats1[i].f = stats1[i].f;
    new_node->stats1[i].d = stats1[i].d;
  }
  new_node->stats2 = (Stats_Ptr)malloc(num_of_columns2 *sizeof(struct Stats));
  for(int i = 0; i < num_of_columns2; i++) {
    new_node->stats2[i].l = stats2[i].l;
    new_node->stats2[i].u = stats2[i].u;
    new_node->stats2[i].f = stats2[i].f;
    new_node->stats2[i].d = stats2[i].d;
  }

  new_node->num_of_columns1 = num_of_columns1;
  new_node->num_of_columns2 = num_of_columns2;
  new_node->next=NULL;
  return new_node;
}

//static void Insert_In_Empty_Queue(Join_Ptr Join, Execution_Queue_Ptr Execution_Queue) { 
static void Insert_In_Empty_Queue(Join_Ptr Join, Execution_Queue_Ptr Execution_Queue, \
Stats_Ptr stats1, Stats_Ptr stats2, uint64_t num_of_columns1, uint64_t num_of_columns2){
  Execution_Queue->head=Create_New_Node(Join, stats1, stats2, num_of_columns1, num_of_columns2);
  //Execution_Queue->head=Create_New_Node(Join);
  Execution_Queue->tail=Execution_Queue->head;
}

//static void Insert_At_End(Join_Ptr Join,Execution_Queue_Ptr Execution_Queue) {
static void Insert_At_End(Join_Ptr Join,Execution_Queue_Ptr Execution_Queue, \
Stats_Ptr stats1, Stats_Ptr stats2, uint64_t num_of_columns1, uint64_t num_of_columns2){
  Execution_Queue_Node_Ptr temp = Execution_Queue->tail;
  Execution_Queue_Node_Ptr new_node = Create_New_Node(Join, stats1, stats2, num_of_columns1, num_of_columns2);
  //Execution_Queue_Node_Ptr new_node = Create_New_Node(Join);

  temp->next=new_node;
  Execution_Queue->tail=new_node;
}

//void Insert_Node(Join_Ptr Join, Execution_Queue_Ptr Execution_Queue) {
void Insert_Node(Join_Ptr Join, Execution_Queue_Ptr Execution_Queue,\
Stats_Ptr stats1, Stats_Ptr stats2, uint64_t num_of_columns1, uint64_t num_of_columns2){
  if(Execution_Queue->head==NULL)
    Insert_In_Empty_Queue(Join,Execution_Queue, stats1, stats2, num_of_columns1, num_of_columns2);
    //Insert_In_Empty_Queue(Join,Execution_Queue);
  else
    Insert_At_End(Join,Execution_Queue, stats1, stats2, num_of_columns1, num_of_columns2);
    //Insert_At_End(Join,Execution_Queue);
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

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

void Set_Join_stats1_l(Execution_Queue_Node_Ptr Node, int i, uint64_t l) {
  Node->stats1[i].l = l;
}
void Set_Join_stats1_u(Execution_Queue_Node_Ptr Node, int i, uint64_t u) {
  Node->stats1[i].u = u;
}
void Set_Join_stats1_f(Execution_Queue_Node_Ptr Node, int i, int64_t f) {
  Node->stats1[i].f = f;
}
void Set_Join_stats1_d(Execution_Queue_Node_Ptr Node, int i, int64_t d) {
  Node->stats1[i].d = d;
}
void Set_Join_stats2_l(Execution_Queue_Node_Ptr Node, int i, uint64_t l) {
  Node->stats2[i].l = l;
}
void Set_Join_stats2_u(Execution_Queue_Node_Ptr Node, int i, uint64_t u) {
  Node->stats2[i].u = u;
}
void Set_Join_stats2_f(Execution_Queue_Node_Ptr Node, int i, int64_t f) {
  Node->stats2[i].f = f;
}
void Set_Join_stats2_d(Execution_Queue_Node_Ptr Node, int i, int64_t d) {
  Node->stats2[i].d = d;
}


uint64_t Get_Node_num_of_columns1(Execution_Queue_Node_Ptr Node) {
  return Node->num_of_columns1;
}
uint64_t Get_Node_num_of_columns2(Execution_Queue_Node_Ptr Node) {
  return Node->num_of_columns2;
}
Stats_Ptr Get_Node_stats1(Execution_Queue_Node_Ptr Node) {
  return Node->stats1;
}
Stats_Ptr Get_Node_stats2(Execution_Queue_Node_Ptr Node) {
  return Node->stats2;
}




uint64_t Get_Join_stats1_l(Execution_Queue_Node_Ptr Node, int i) {
  return Node->stats1[i].l;
}
uint64_t Get_Join_stats1_u(Execution_Queue_Node_Ptr Node, int i) {
  return Node->stats1[i].u;
}
int64_t Get_Join_stats1_f(Execution_Queue_Node_Ptr Node, int i) {
  return Node->stats1[i].f;
}
int64_t Get_Join_stats1_d(Execution_Queue_Node_Ptr Node, int i) {
  return Node->stats1[i].d;
}

uint64_t Get_Join_stats2_l(Execution_Queue_Node_Ptr Node, int i) {
  return Node->stats2[i].l;
}
uint64_t Get_Join_stats2_u(Execution_Queue_Node_Ptr Node, int i) {
  return Node->stats2[i].u;
}
int64_t Get_Join_stats2_f(Execution_Queue_Node_Ptr Node, int i) {
  return Node->stats2[i].f;
}
int64_t Get_Join_stats2_d(Execution_Queue_Node_Ptr Node, int i) {
  return Node->stats2[i].d;
}
