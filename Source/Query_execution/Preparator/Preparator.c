#include "Preparator.h"
#include "../Execution_Queue/Execution_Queue.h"
#include "../Join_Struct/Join_Struct.h"

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


