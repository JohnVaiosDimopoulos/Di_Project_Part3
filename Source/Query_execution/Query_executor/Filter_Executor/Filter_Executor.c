#include "Filter_Executor.h"
#include "string.h"

struct Tuple{
  uint64_t element;
  uint64_t row_id;
};



static int Execute(Tuple_Ptr *New, Shell_Ptr Shell, Filter_Ptr Filter, FILE *fp) {

  //get filter content
  int rel = Get_Filter_Relation(Filter);
  int col = Get_Filter_Column(Filter);
  char type[2];
  strcpy(type, Get_Type(Filter));
  int con = Get_Constant(Filter);

  int cnt = 0;
  for(int i =0; i < Get_num_of_tuples(Shell); i++){

    Tuple_Ptr current = Get_Shell_Array_by_index(Shell, col, i);
	uint64_t data_to_check = Get_Data(current);
	//printf("%llu\n", data_to_check);
    
    switch(type[0]) {
      case '<':
        if(data_to_check < con) {
	      for(int j =0; j < Get_num_of_columns(Shell); j++) {
            Tuple_Ptr Tuples = Get_Shell_Array_by_index(Shell, j, i);
	        uint64_t data = Get_Data(Tuples);
            uint64_t row = Get_Row_id(Tuples);
            New[j][cnt].element = data;
            New[j][cnt].row_id = row;
		  }
          cnt++;
        }
        break;
      case '>':
        if(data_to_check > con) {
	      for(int j =0; j < Get_num_of_columns(Shell); j++) {
            Tuple_Ptr Tuples = Get_Shell_Array_by_index(Shell, j, i);
	        uint64_t data = Get_Data(Tuples);
            uint64_t row = Get_Row_id(Tuples);
            New[j][cnt].element = data;
            New[j][cnt].row_id = row;
		  }
          cnt++;
        }
        break;
      case '=':
        if(data_to_check ==  con) {
	      for(int j =0; j < Get_num_of_columns(Shell); j++) {
            Tuple_Ptr Tuples = Get_Shell_Array_by_index(Shell, j, i);
	        uint64_t data = Get_Data(Tuples);
            uint64_t row = Get_Row_id(Tuples);
            New[j][cnt].element = data;
            New[j][cnt].row_id = row;
		  }
          cnt++;
        }
        break;
    }
  }
  return cnt;
}

void Execute_Filters(Table_Ptr Table, Parsed_Query_Ptr Parsed_Query) {
  int num_of_filters = Get_Num_of_Filters(Parsed_Query);

  if(num_of_filters) {
	FILE *fp = fopen("test", "w");
    for (int i = 0; i < num_of_filters; i++) {
      Filter_Ptr Filter = Get_Filter_by_index(Get_Filters(Parsed_Query), i);
      int rel = Get_Filter_Relation(Filter);
      Shell_Ptr Shell = Get_Shell_by_index(Get_Table_Array(Table), rel);
      uint64_t num_of_tuples = Get_num_of_tuples(Shell);
      uint64_t num_of_columns = Get_num_of_columns(Shell);

      //allocate array
      Tuple_Ptr *New = (Tuple_Ptr*)malloc(num_of_columns * sizeof(Tuple_Ptr));
      New[0]= (Tuple_Ptr)malloc((num_of_columns * num_of_tuples)* sizeof(struct Tuple));
      Setup_Column_Pointers(New, num_of_columns, num_of_tuples);

      int tuples = Execute(New, Shell, Filter, fp);

      Tuple_Ptr *temp = Get_Shell_Array(Shell);
	  Set_Shell_Array(Shell, New);
	  Set_Shell_num_of_tuples(Shell, tuples);
	  //delete old shell
      free(temp[0]);
      free(temp);

	  //just for checking
//	  int j = 0;
//      fprintf(fp, "REL %d\n", rel);
//      for(int i =0; i< tuples; i++){
//        for(int j =0; j < num_of_columns; j++){
//          fprintf(fp,"(%llu)", New[j][i].row_id);
//          fprintf(fp, "%llu|", New[j][i].element);
//		}
//        fprintf(fp, "\n");
//	  }
//        j++;
//        if(j == num_of_columns) {
//          fprintf(fp, "\n");
//          j = 0;
//        }
//      }
    }
	fclose(fp);
	return;
  }
  printf("QUERY HAS NO FILTERS\n");
}



