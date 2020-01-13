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


static void Update_Stats(Shell_Ptr Shell, char *type, int col, int con) {

  //for(int i =0; i < Get_num_of_columns(Shell); i++){
  switch(*type) {
    case '<':
      printf("<\n");
      printf("l = %llu\n", Get_Column_l(Shell, col));
      printf("u = %llu\n", Get_Column_u(Shell, col));
      printf("f = %llu\n", Get_Column_f(Shell, col));
      printf("d = %llu\n", Get_Column_d(Shell, col));
      Set_Column_l(Shell, col, Get_Column_l(Shell, col));
      Set_Column_u(Shell, col, con);
      Set_Column_f(Shell, col, Get_Column_f(Shell, col));
      Set_Column_d(Shell, col, Get_Column_d(Shell, col));
      printf("AFTER\n");
      printf("l = %llu\n", Get_Column_l(Shell, col));
      printf("u = %llu\n", Get_Column_u(Shell, col));
      printf("f = %llu\n", Get_Column_f(Shell, col));
      printf("d = %llu\n", Get_Column_d(Shell, col));
      break;
    case '>':
      printf(">\n");
      printf("l = %llu\n", Get_Column_l(Shell, col));
      printf("u = %llu\n", Get_Column_u(Shell, col));
      printf("f = %llu\n", Get_Column_f(Shell, col));
      printf("d = %llu\n", Get_Column_d(Shell, col));
      Set_Column_l(Shell, col, con);
      Set_Column_u(Shell, col, Get_Column_u(Shell, col));
      Set_Column_f(Shell, col, Get_Column_f(Shell, col));
      Set_Column_d(Shell, col, Get_Column_d(Shell, col));
      printf("AFTER\n");
      printf("l = %llu\n", Get_Column_l(Shell, col));
      printf("u = %llu\n", Get_Column_u(Shell, col));
      printf("f = %llu\n", Get_Column_f(Shell, col));
      printf("d = %llu\n", Get_Column_d(Shell, col));
      break;
    case '=':
      printf("=\n");
      printf("l = %llu\n", Get_Column_l(Shell, col));
      printf("u = %llu\n", Get_Column_u(Shell, col));
      printf("f = %llu\n", Get_Column_f(Shell, col));
      printf("d = %llu\n", Get_Column_d(Shell, col));
      Set_Column_l(Shell, col, con);
      Set_Column_u(Shell, col, con);
      Set_Column_f(Shell, col, Get_Column_f(Shell, col));
      Set_Column_d(Shell, col, 1);
      printf("AFTER\n");
      printf("l = %llu\n", Get_Column_l(Shell, col));
      printf("u = %llu\n", Get_Column_u(Shell, col));
      printf("f = %llu\n", Get_Column_f(Shell, col));
      printf("d = %llu\n", Get_Column_d(Shell, col));
      break;
  }
 // }

}

void Execute_Filters(Table_Ptr Table, Parsed_Query_Ptr Parsed_Query) {
  int num_of_filters = Get_Num_of_Filters(Parsed_Query);

  if(num_of_filters) {
	FILE *fp = fopen("test", "w");
    for (int i = 0; i < num_of_filters; i++) {
      Filter_Ptr Filter = Get_Filter_by_index(Get_Filters(Parsed_Query), i);
      int rel = Get_Filter_Relation(Filter);
	  printf("REL = %d\n", rel);
      Shell_Ptr Shell = Get_Shell_by_index(Get_Table_Array(Table), rel);
      uint64_t num_of_tuples = Get_num_of_tuples(Shell);
      uint64_t num_of_columns = Get_num_of_columns(Shell);
	  printf("%llu %llu\n", num_of_tuples, num_of_columns);

      //allocate array
      Tuple_Ptr *New = (Tuple_Ptr*)malloc(num_of_columns * sizeof(Tuple_Ptr));
      New[0]= (Tuple_Ptr)malloc((num_of_columns * num_of_tuples)* sizeof(struct Tuple));
      Setup_Column_Pointers(New, num_of_columns, num_of_tuples);

      int tuples = Execute(New, Shell, Filter, fp);

      //point to the new (smaller) array
      Tuple_Ptr *temp = Get_Shell_Array(Shell);
	  Set_Shell_Array(Shell, New);
	  //delete old shell
      free(temp[0]);
      free(temp);
	  //update shell stats
	  printf("%llu %llu\n", Get_num_of_tuples(Shell), Get_num_of_columns(Shell));
	  Update_Stats(Shell, Get_Type(Filter), Get_Filter_Column(Filter), Get_Constant(Filter));

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



