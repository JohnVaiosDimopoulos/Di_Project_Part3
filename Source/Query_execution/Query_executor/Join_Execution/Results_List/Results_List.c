#include "Results_List.h"
#include <stdlib.h>




struct Result_List{
  long num_of_nodes;
  long num_of_results;
  Node_Ptr start;
  Node_Ptr last;
};


static int Is_Empty(List_Ptr List){
  if(List->start->counter==0)
    return 1;
  return 0;
}

void Print_List(List_Ptr List, FILE *fp) {

  if(Is_Empty(List)){
    fprintf(fp,"No results found\n");
    printf("No results found\n");
    return;
  }

  int counter=1;
  Node_Ptr node = List->start;

  while (node!=NULL){
    for(int i = 0; i < LIST_SIZE;i++){
      if(node->Array[i][1] == 0)
        continue;
      fprintf(fp, "%d: Table1: %llu, Table2: %llu\n", counter, node->Array[i][0], node->Array[i][1]);
      counter++;
    }
    node=node->next;
  }
}


Node_Ptr New_Node() {
  Node_Ptr Pnode = (Node_Ptr)malloc(sizeof(struct List_node));
  Pnode->counter = 0;
  Pnode->next = NULL;
  return Pnode;
}

List_Ptr Create_and_Initialize_List() {
  List_Ptr List = (List_Ptr)malloc(sizeof(struct Result_List));
  List->start = New_Node();
  List->last = List->start;
  List->num_of_nodes=0;
  List->num_of_results=0;
  return List;
}

void Insert_Record(List_Ptr List, uint64_t id_1, uint64_t id_2) {
  Node_Ptr Pnode = List->last;
  List->num_of_results++;

  if(Pnode->counter == LIST_SIZE) {
    Pnode->next = New_Node();
    List->num_of_nodes++;
    Pnode = Pnode->next;
  }
  List->last = Pnode;

  Pnode->Array[Pnode->counter][0] = id_1;
  Pnode->Array[Pnode->counter][1] = id_2;

  Pnode->counter++;
}

void Delete_List(List_Ptr List){
  Node_Ptr Pnode = List->start;
  Node_Ptr temp = Pnode;
  while(Pnode) {
    temp = temp->next;
    free(Pnode);
    Pnode = temp;
  }
  free(List);
}

int Get_num_of_results(List_Ptr List){
  return List->num_of_results;
}


Node_Ptr Get_head(List_Ptr List){
  return List->start;
}
