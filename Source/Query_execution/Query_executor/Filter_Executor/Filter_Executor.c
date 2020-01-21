#include "Filter_Executor.h"
#include "string.h"

//struct Tuple{
//  uint64_t element;
//  uint64_t row_id;
//};



static int Execute(Tuple_Ptr *New, Shell_Ptr Shell, Filter_Ptr Filter) {

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

static void Update_Stats(Shell_Ptr Shell, Filter_Ptr Filter, int tuples) {
  char *type = Get_Type(Filter);
  int col = Get_Filter_Column(Filter);
  int con = Get_Constant(Filter);

//  for(int i =0; i < Get_num_of_columns(Shell); i++){
//    printf("BEFORE\n");
//    printf("l = %llu\n", Get_Column_l(Shell, i));
//    printf("u = %llu\n", Get_Column_u(Shell, i));
//    printf("f = %llu\n", Get_Column_f(Shell, i));
//    printf("d = %llu\n", Get_Column_d(Shell, i));
//  }
  uint64_t la = Get_Column_l(Shell, col);
  uint64_t ua = Get_Column_u(Shell, col);
  int64_t f, fa = Get_Column_f(Shell, col);
  int64_t d, da = Get_Column_d(Shell, col);
  float fraction;
  switch(*type) {
    case '<':
//      printf("<\n");
	  fraction = (con - Get_Column_l(Shell, col)) / (float)(ua - Get_Column_l(Shell, col));
	  f = fraction * fa;
      for(int i =0; i < Get_num_of_columns(Shell); i++){
		if(i == col) {
          Set_Column_u(Shell, col, con);
		  d = fraction * Get_Column_d(Shell, col);; 
          Set_Column_d(Shell, col, d);
		} else {
		  int64_t fc = Get_Column_f(Shell, i);
		  int64_t dc = Get_Column_d(Shell, i);
		  float f_fraction = f / (float)fa;
          float p = power((1 - f_fraction), (fc / dc));
          Set_Column_d(Shell, i, dc * (1 - p));
		}
        Set_Column_f(Shell, i, f);
	  }
      break;
    case '>':
//      printf(">\n");
	  fraction = (Get_Column_u(Shell, col) - con) / (float)(Get_Column_u(Shell, col) - la);
      f = fraction * fa;
      for(int i =0; i < Get_num_of_columns(Shell); i++){
		if(i == col) {
          Set_Column_l(Shell, col, con);
		  d = fraction * Get_Column_d(Shell, col); 
          Set_Column_d(Shell, col, d);
		} else {
		  int64_t fc = Get_Column_f(Shell, i);
		  int64_t dc = Get_Column_d(Shell, i);
		  float f_fraction = f / (float)fa;
          float p = power((1 - f_fraction), (fc / dc));
//		  printf("p = %f\n", p);
          Set_Column_d(Shell, i, dc * (1 - p));
		}
        Set_Column_f(Shell, i, f);
	  }
      break;
    case '=':
//      printf("=\n");
	  if(Get_d_array(Shell)[col][con - la] == true) {
//	    printf("\t\t\tIF\n");
	    f = (int64_t)(fa / da);
		d = 1;
	  } else {
//	    printf("\t\t\tELSE\n");
		f = 0; d = 0;
	  }
      for(int i =0; i < Get_num_of_columns(Shell); i++){
		if(i == col) {
          Set_Column_l(Shell, col, con);
          Set_Column_u(Shell, col, con);
          Set_Column_d(Shell, col, d);
		} else {
		  int64_t fc = Get_Column_f(Shell, i);
		  int64_t dc = Get_Column_d(Shell, i);
		  float f_fraction = f / (float)fa;
          float p = power((1 - f_fraction), (fc / dc));
          Set_Column_d(Shell, i, dc * (1 - p));
		}
        Set_Column_f(Shell, i, f);
	  }
      break;
  }
//  for(int i =0; i < Get_num_of_columns(Shell); i++){
//    printf("AFTER\n");
//    printf("l = %llu\n", Get_Column_l(Shell, i));
//    printf("u = %llu\n", Get_Column_u(Shell, i));
//    printf("f = %llu\n", Get_Column_f(Shell, i));
//    printf("d = %llu\n", Get_Column_d(Shell, i));
//  }

}

void Execute_Filters(Table_Ptr Table, Parsed_Query_Ptr Parsed_Query) {
  int num_of_filters = Get_Num_of_Filters(Parsed_Query);

  if(num_of_filters) {
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

      int tuples = Execute(New, Shell, Filter);

      //point to the new (smaller) array
      Tuple_Ptr *temp = Get_Shell_Array(Shell);
	  Set_Shell_Array(Shell, New);
	  Set_Shell_num_of_tuples(Shell, tuples);

	  //delete old shell
      free(temp[0]);
      free(temp);
	  //update shell stats
	  //printf("%llu %llu\n", Get_num_of_tuples(Shell), Get_num_of_columns(Shell));
	  Update_Stats(Shell, Filter, tuples);


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
	return;
  }
  printf("QUERY HAS NO FILTERS\n");
}
