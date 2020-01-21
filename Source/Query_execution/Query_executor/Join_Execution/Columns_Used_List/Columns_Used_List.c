#include "Columns_Used_List.h"
#include <stdlib.h>

struct Columns_list_node{
  int col;
  int rel;
  Columns_list_node_Ptr next;
};

struct Used_Columns_List{
  Columns_list_node_Ptr head;
};


Used_Columns_List_Ptr Create_Columns_list() {
  Used_Columns_List_Ptr List = malloc(sizeof(struct Used_Columns_List));
  List->head = NULL;
  return List;
}

void Delete_Columns_list(Used_Columns_List_Ptr List){

  Columns_list_node_Ptr temp = List->head;
  Columns_list_node_Ptr del = temp;

  while (temp!=NULL){
    temp = temp->next;
    free(del);
    del = temp;
  }
  free(List);
}


void Insert_Column(Used_Columns_List_Ptr List, int rel, int col){
  Columns_list_node_Ptr new_node = malloc(sizeof(struct Columns_list_node));
  new_node->rel = rel;
  new_node->col = col;
  new_node->next = List->head;
  List->head = new_node;
}

int is_in_list(Used_Columns_List_Ptr List, int rel, int col) {
  if(List->head == NULL)
    return 0;

  Columns_list_node_Ptr  temp = List->head;
  while (temp!=NULL){
    if(temp->rel == rel && temp->col == col)
      return 1;
    temp = temp->next;
  }
  return 0;
}

