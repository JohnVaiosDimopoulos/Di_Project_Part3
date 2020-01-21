#ifndef MULTI_JOIN_COLUMNS_USED_LIST_H
#define MULTI_JOIN_COLUMNS_USED_LIST_H

typedef struct Columns_list_node* Columns_list_node_Ptr;
typedef struct Used_Columns_List* Used_Columns_List_Ptr;
Used_Columns_List_Ptr Create_Columns_list();
void Delete_Columns_list(Used_Columns_List_Ptr List);
void Insert_Column(Used_Columns_List_Ptr List, int rel, int col);
int is_in_list(Used_Columns_List_Ptr List, int rel, int col);
#endif //MULTI_JOIN_COLUMNS_USED_LIST_H
