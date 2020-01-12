CC = gcc
CFLAGS  = -g -Wall

main: main.o Initializer/Argument_Manager/Argument_Manager.o Initializer/Argument_Data/Arguments_Data.o Initializer/Table_Allocator/Table_Allocator.o Util/Utilities.o Work_Reader/Work_Reader.o Work_Executor/Work_Executor.o Batch/Batch.o Query/Query.o Query_execution/Preparator/Preparator.o Query_execution/Query_parser/Parse_Relations/Parse_Relations.o Query_execution/Query_parser/Parse_Predicates/Parse_Predicates.o Query_execution/Query_parser/Parse_Projections/Parse_Projections.o Query_execution/Query_parser/Query_parser.o Query_execution/Filter/Filter.o Query_execution/Join_Struct/Join_Struct.o Query_execution/Projection/Projection.o Query_execution/Execution_Queue/Execution_Queue.o Query_execution/Query_executor/Query_Executor.o Query_execution/Query_executor/Filter_Executor/Filter_Executor.o Query_execution/Query_executor/Join_Execution/Histogram/Histogram.o Query_execution/Query_executor/Join_Execution/Join/Join.o Query_execution/Query_executor/Join_Execution/Prefix_sum/Prefix_Sum.o Query_execution/Query_executor/Join_Execution/Relation_Creator/Relation_Creator.o Query_execution/Query_executor/Join_Execution/Relation_Sorting/Relation_Sorting.o Query_execution/Query_executor/Join_Execution/Results_List/Results_List.o Query_execution/Query_executor/Join_Execution/Join_Execution.o Query_execution/Query_executor/Projection_Executor/Projection_Executor.o
	$(CC) $(CFLAGS) -o main main.o Argument_Manager.o Arguments_Data.o Table_Allocator.o Utilities.o Work_Reader.o Work_Executor.o Batch.o Query.o Preparator.o Parse_Relations.o Parse_Predicates.o Parse_Projections.o Query_parser.o Filter.o Join_Struct.o Projection.o Execution_Queue.o Query_Executor.o Filter_Executor.o Histogram.o Join.o Prefix_Sum.o Relation_Creator.o Relation_Sorting.o Results_List.o Join_Execution.o Projection_Executor.o

main.o: main.c Initializer/Argument_Manager/Argument_Manager.h Initializer/Argument_Data/Arguments_Data.h Initializer/Table_Allocator/Table_Allocator.h Util/Utilities.h Work_Reader/Work_Reader.h Work_Executor/Work_Executor.h Batch/Batch.h Query/Query.h Query_execution/Preparator/Preparator.h Query_execution/Query_parser/Parse_Relations/Parse_Relations.h Query_execution/Query_parser/Parse_Predicates/Parse_Predicates.h Query_execution/Query_parser/Parse_Projections/Parse_Projections.h Query_execution/Query_parser/Query_parser.h Query_execution/Filter/Filter.h Query_execution/Join_Struct/Join_Struct.h Query_execution/Projection/Projection.h Query_execution/Query_executor/Query_Executor.h Query_execution/Execution_Queue/Execution_Queue.h Query_execution/Query_executor/Filter_Executor/Filter_Executor.h Query_execution/Query_executor/Join_Execution/Histogram/Histogram.h Query_execution/Query_executor/Join_Execution/Join/Join.h Query_execution/Query_executor/Join_Execution/Prefix_sum/Prefix_Sum.h Query_execution/Query_executor/Join_Execution/Relation_Creator/Relation_Creator.h Query_execution/Query_executor/Join_Execution/Relation_Sorting/Relation_Sorting.h Query_execution/Query_executor/Join_Execution/Results_List/Results_List.h Query_execution/Query_executor/Join_Execution/Join_Execution.h Query_execution/Query_executor/Projection_Executor/Projection_Executor.h
	$(CC) -c main.c 

Util/Utilities.o: Util/Utilities.c Util/Utilities.h
	$(CC) -c Util/Utilities.c

######################################################
######################################################
######################################################

Query_execution/Query_executor/Join_Execution/Histogram/Histogram.o: Query_execution/Query_executor/Join_Execution/Histogram/Histogram.c Query_execution/Query_executor/Join_Execution/Histogram/Histogram.h
	$(CC) -c Query_execution/Query_executor/Join_Execution/Histogram/Histogram.c

Query_execution/Query_executor/Join_Execution/Prefix_sum/Prefix_Sum.o: Query_execution/Query_executor/Join_Execution/Prefix_sum/Prefix_Sum.c Query_execution/Query_executor/Join_Execution/Prefix_sum/Prefix_Sum.h
	$(CC) -c Query_execution/Query_executor/Join_Execution/Prefix_sum/Prefix_Sum.c

Query_execution/Query_executor/Join_Execution/Join/Join.o: Query_execution/Query_executor/Join_Execution/Join/Join.c Query_execution/Query_executor/Join_Execution/Join/Join.h
	$(CC) -c Query_execution/Query_executor/Join_Execution/Join/Join.c

Query_execution/Query_executor/Join_Execution/Relation_Creator/Relation_Creator.o: Query_execution/Query_executor/Join_Execution/Relation_Creator/Relation_Creator.c Query_execution/Query_executor/Join_Execution/Relation_Creator/Relation_Creator.h
	$(CC) -c Query_execution/Query_executor/Join_Execution/Relation_Creator/Relation_Creator.c

Query_execution/Query_executor/Join_Execution/Relation_Sorting/Relation_Sorting.o: Query_execution/Query_executor/Join_Execution/Relation_Sorting/Relation_Sorting.c Query_execution/Query_executor/Join_Execution/Relation_Sorting/Relation_Sorting.h
	$(CC) -c Query_execution/Query_executor/Join_Execution/Relation_Sorting/Relation_Sorting.c

Query_execution/Query_executor/Join_Execution/Results_List/Results_List.o: Query_execution/Query_executor/Join_Execution/Results_List/Results_List.c Query_execution/Query_executor/Join_Execution/Results_List/Results_List.h
	$(CC) -c Query_execution/Query_executor/Join_Execution/Results_List/Results_List.c

Query_execution/Query_executor/Join_Execution/Join_Execution.o: Query_execution/Query_executor/Join_Execution/Join_Execution.c Query_execution/Query_executor/Join_Execution/Join_Execution.h
	$(CC) -c Query_execution/Query_executor/Join_Execution/Join_Execution.c

Query_execution/Query_executor/Projection_Executor/Projection_Executor.o: Query_execution/Query_executor/Projection_Executor/Projection_Executor.c Query_execution/Query_executor/Projection_Executor/Projection_Executor.h
	$(CC) -c Query_execution/Query_executor/Projection_Executor/Projection_Executor.c

######################################################

Query_execution/Preparator/Preparator.o: Query_execution/Preparator/Preparator.c Query_execution/Preparator/Preparator.h        
	$(CC) -c Query_execution/Preparator/Preparator.c

######################################################

Query_execution/Execution_Queue/Execution_Queue.o: Query_execution/Execution_Queue/Execution_Queue.c Query_execution/Execution_Queue/Execution_Queue.h
	$(CC) -c Query_execution/Execution_Queue/Execution_Queue.c

Query_execution/Query_executor/Query_Executor.o: Query_execution/Query_executor/Query_Executor.c Query_execution/Query_executor/Query_Executor.h
	$(CC) -c Query_execution/Query_executor/Query_Executor.c

Query_execution/Query_executor/Filter_Executor/Filter_Executor.o: Query_execution/Query_executor/Filter_Executor/Filter_Executor.c Query_execution/Query_executor/Filter_Executor/Filter_Executor.h
	$(CC) -c Query_execution/Query_executor/Filter_Executor/Filter_Executor.c

######################################################

Query_execution/Filter/Filter.o: Query_execution/Filter/Filter.c Query_execution/Filter/Filter.h
	$(CC) -c Query_execution/Filter/Filter.c

Query_execution/Join_Struct/Join_Struct.o: Query_execution/Join_Struct/Join_Struct.c Query_execution/Join_Struct/Join_Struct.h
	$(CC) -c Query_execution/Join_Struct/Join_Struct.c

Query_execution/Projection/Projection.o: Query_execution/Projection/Projection.c Query_execution/Projection/Projection.h
	$(CC) -c Query_execution/Projection/Projection.c

######################################################

Query_execution/Query_parser/Query_parser.o: Query_execution/Query_parser/Query_parser.c Query_execution/Query_parser/Query_parser.h 
	$(CC) -c Query_execution/Query_parser/Query_parser.c    

Query_execution/Query_parser/Parse_Relations/Parse_Relations.o: Query_execution/Query_parser/Parse_Relations/Parse_Relations.c Query_execution/Query_parser/Parse_Relations/Parse_Relations.h 
	$(CC) -c Query_execution/Query_parser/Parse_Relations/Parse_Relations.c

Query_execution/Query_parser/Parse_Predicates/Parse_Predicates.o: Query_execution/Query_parser/Parse_Predicates/Parse_Predicates.c Query_execution/Query_parser/Parse_Predicates/Parse_Predicates.h 
	$(CC) -c Query_execution/Query_parser/Parse_Predicates/Parse_Predicates.c    

Query_execution/Query_parser/Parse_Projections/Parse_Projections.o: Query_execution/Query_parser/Parse_Projections/Parse_Projections.c Query_execution/Query_parser/Parse_Projections/Parse_Projections.h 
	$(CC) -c Query_execution/Query_parser/Parse_Projections/Parse_Projections.c 

######################################################
######################################################
######################################################

Query/Query.o: Query/Query.c Query/Query.h
	$(CC) -c Query/Query.c

Batch/Batch.o: Batch/Batch.c Batch/Batch.h
	$(CC) -c Batch/Batch.c

Work_Executor/Work_Executor.o: Work_Executor/Work_Executor.c Work_Executor/Work_Executor.h
	$(CC) -c Work_Executor/Work_Executor.c

######################################################
######################################################
######################################################

Work_Reader/Work_Reader.o: Work_Reader/Work_Reader.c Work_Reader/Work_Reader.h
	$(CC) -c Work_Reader/Work_Reader.c

Initializer/Table_Allocator/Table_Allocator.o: Initializer/Table_Allocator/Table_Allocator.c Initializer/Table_Allocator/Table_Allocator.h
	$(CC) -c Initializer/Table_Allocator/Table_Allocator.c 

Initializer/Argument_Data/Arguments_Data.o: Initializer/Argument_Data/Arguments_Data.c Initializer/Argument_Data/Arguments_Data.h
	$(CC) -c Initializer/Argument_Data/Arguments_Data.c 

Initializer/Argument_Manager/Argument_Manager.o: Initializer/Argument_Manager/Argument_Manager.c Initializer/Argument_Manager/Argument_Manager.h
	$(CC) -c Initializer/Argument_Manager/Argument_Manager.c 
	
clean:
	$(RM) main *.o 
