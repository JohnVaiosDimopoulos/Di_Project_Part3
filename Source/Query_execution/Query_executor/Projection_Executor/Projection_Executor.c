#include "Projection_Executor.h"
#include "../Query_Executor.h"
#include "../../Query_parser/Query_parser.h"
#include "../../Preparator/Preparator.h"
#include "../Filter_Executor/Filter_Executor.h"
#include "../Join_Execution/Join_Execution.h"
#include <stdlib.h>

void Execute_Projections(Intermediate_Result_Ptr Res, Parsed_Query_Ptr Parsed_Query, Table_Ptr Table, FILE *fp) {

  int num_of_proj = Get_Num_of_Projections(Parsed_Query);
  Projection_Ptr Proj = Get_Projections(Parsed_Query);

  for(int i = 0; i < num_of_proj; i++) {

    Projection_Ptr current_proj = Get_Proj_by_index(Proj, i);
	int rel = *Get_Projection_Relation(current_proj);
    int col = *Get_Projection_Column(current_proj);


	if(Res) {
	  int num_of_res = Res->num_of_results;
      struct Result** row_ids = Res->row_ids;

      int *Relation = Get_Relations(Parsed_Query);
	  int initial_rel = Relation[rel];


	  Shell_Ptr Shell = Get_Table_Array(Table);
	  Shell_Ptr current_shell = Get_Shell_by_index(Shell, initial_rel);

	  uint64_t sum = 0;

      //if this relation is in the intermediate struct
	  if(Res->relations_in_result[rel]) {

        int index;
        for(int k = 0; k < Res->num_of_relations; k++) {
          if(Res->row_ids[0][k].relation == rel) {
		    index = Res->row_ids[0][k].relation;
		    break;
		  }
		}
	    //copy result_row_ids
		uint64_t* result_row_ids=malloc(num_of_res* sizeof(uint64_t));
        for(int j = 0; j < num_of_res; j++){
          result_row_ids[j] = row_ids[j][index].row_id;
	      Tuple_Ptr current_tuple = Get_Shell_Array_by_index(current_shell, col, result_row_ids[j]);
	      sum += current_tuple->element;
      }
        free(result_row_ids);
	  } else {
        printf("doesnt exist\n");
		return;
      }
      fprintf(fp, "%llu ", sum);


    } else fprintf(fp, "NULL ");
  }
}
