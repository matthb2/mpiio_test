#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <mpi.h>

#define RATIO 16
#define DATASIZE 1024*1024*20
#define LOC ((rank/RATIO)*DATASIZE)

int main(int argc, char** argv)
{
	int size;
	int rank;
	char* file_name;
	char* data;
	char* rdata;
	size_t i;
	MPI_Comm local_comm;
	MPI_File file_handle;
	MPI_Status status;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	assert(size%RATIO==0);
	data = (char*) malloc(sizeof(char)*DATASIZE);
	rdata = (char*) malloc(sizeof(char)*DATASIZE);
	for(i=0;i<DATASIZE;i++)
		*(data+i) = 0xFF;
	*(data+DATASIZE-2) = 0xBE;
	*(data+DATASIZE-1) = 0xEF;
//	arc4random_buf(data, DATASIZE);
	MPI_Comm_split(MPI_COMM_WORLD, rank%RATIO, rank, &local_comm);
	asprintf(&file_name, "data-%d.dat", rank%RATIO);
	MPI_File_open(local_comm, file_name, 
			MPI_MODE_WRONLY|MPI_MODE_CREATE, MPI_INFO_NULL,
			&file_handle);
	//MPI_File_write_at(file_handle, DATASIZE*rank, 
	//		 data, DATASIZE, MPI_CHAR, &status);

	MPI_File_write_at_all_begin(file_handle, LOC,
		       	data, DATASIZE, MPI_BYTE);
	MPI_File_write_at_all_end(file_handle, data, &status);

	MPI_File_close(&file_handle);
	MPI_Barrier(MPI_COMM_WORLD);
	printf("INFO: Starting to Read back Files\n");
	MPI_File_open(local_comm, file_name, MPI_MODE_RDONLY, MPI_INFO_NULL,
			&file_handle);
	printf("INFO: File Opened\n");
	MPI_File_read_at_all_begin(file_handle, LOC , rdata, DATASIZE, MPI_BYTE);
	printf("INFO: Read Begin\n");
	MPI_File_read_at_all_end(file_handle, rdata, &status);
	printf("INFO: Read End\n");
	MPI_File_close(&file_handle);
	for(i=0;i<DATASIZE;i++)
	{
		assert(*(data+i) == *(rdata+i));
	}
	free(data);
	free(rdata);
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
}
