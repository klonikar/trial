#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#define handle_error(msg) \
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

// Row wise data store
#define MAX_NAME_LEN 64
typedef struct {
    unsigned long counter;
    char name[MAX_NAME_LEN];
} ProfilingData;

typedef struct {
    int counters;
    char data[1]; // use the struct hack here...
} ProfilingHeaderAndData;

void
writeCounters(const char *fileName, int counters) {
    int fd = open(fileName, O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IROTH);
    if (fd == -1)
        handle_error("open write");
    struct stat sb;
    if (fstat(fd, &sb) == -1)           /* To obtain file size */
        handle_error("fstat write");
    size_t length = sizeof(int) + counters*sizeof(ProfilingData);
    if(sb.st_size < length) { // extend the length of the file
        /* go to the location corresponding to the last byte */
        if (lseek (fd, length - 1, SEEK_SET) == -1)
            handle_error ("lseek error");
 
        /* write a dummy byte at the last location */
        if (write (fd, "", 1) != 1)
            handle_error ("write error");
    }
    char *addr = (char *) mmap(NULL, length, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED)
        handle_error("mmap write");
    ProfilingHeaderAndData *countersP = (ProfilingHeaderAndData *) addr;
    ProfilingData *data = (ProfilingData *) countersP->data;
    // Set data
    countersP->counters = counters;
    for(int i = 0;i < counters;i++) {
        data[i].counter = i+1;
        snprintf(data[i].name, MAX_NAME_LEN, "counter_%d", i+1);
    }
    munmap(addr, length);
    close(fd);
}

void
readCounters(const char *fileName, int maxCounters) {
    int fd = open(fileName, O_RDONLY);
    if (fd == -1)
        handle_error("open read");
    struct stat sb;
    if (fstat(fd, &sb) == -1)           /* To obtain file size */
        handle_error("fstat read");
    char *addr = (char *) mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED)
        handle_error("mmap read");
    ProfilingHeaderAndData *countersP = (ProfilingHeaderAndData *) addr;
    ProfilingData *data = (ProfilingData *) countersP->data;
    maxCounters = maxCounters < countersP->counters ? maxCounters : countersP->counters;
    for(int i = 0;i < maxCounters;i++) {
        printf("%s: %d\n", data[i].name, data[i].counter);
    }
    munmap(addr, sb.st_size);
    close(fd);
}

typedef struct {
    int numCols;
    int numRows;
} ColumnarMetadata;

typedef struct {
	ColumnarMetadata metaData;
	char data[1]; // struct hack
} ColumnarData;

void
writeColumnarData(const char *fileName, int numCols, int numRows) {
    int fd = open(fileName, O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IROTH);
    if (fd == -1)
        handle_error("open write");
    struct stat sb;
    if (fstat(fd, &sb) == -1)           /* To obtain file size */
        handle_error("fstat write");
    size_t length = sizeof(ColumnarMetadata) + numCols*numRows*sizeof(double);
    if(sb.st_size < length) { // extend the length of the file
        /* go to the location corresponding to the last byte */
        if (lseek (fd, length - 1, SEEK_SET) == -1)
            handle_error ("lseek error");
 
        /* write a dummy byte at the last location */
        if (write (fd, "", 1) != 1)
            handle_error ("write error");
    }
    char *addr = (char *) mmap(NULL, length, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED)
        handle_error("mmap write");
    ColumnarData *dataMetadataP = (ColumnarData *) addr;
    double *dataP = (double *) dataMetadataP->data;
    // Set data
	dataMetadataP->metaData.numCols = numCols;
	dataMetadataP->metaData.numRows = numRows;
    for(int i = 0;i < numCols;i++) {
		double *colDataP = &dataP[i*numRows];
		for(int j = 0;j < numRows;j++) {
			colDataP[j] = (i+1)*(j+1);
	    }
    }
    munmap(addr, length);
    close(fd);
}

// Perform some operation on 1st and 2nd column and store the 
// result in 3rd column
void
modifyColumnarData(const char *fileName) {
    printf("Modifying data using mmap\n");
    int fd = open(fileName, O_RDWR);
    if (fd == -1)
        handle_error("open read");
    struct stat sb;
    if (fstat(fd, &sb) == -1)           /* To obtain file size */
        handle_error("fstat read");

    char *addr = (char *) mmap(NULL, sb.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED)
        handle_error("mmap modify");

    //int ret = madvise(addr, sb.st_size, MADV_SEQUENTIAL);
    //if(ret != 0)
    //    handle_error("madvise modify");

    ColumnarData *dataMetadataP = (ColumnarData *) addr;
    double *dataP = (double *) dataMetadataP->data;
	int numCols = dataMetadataP->metaData.numCols, numRows = dataMetadataP->metaData.numRows;
	printf("numCols: %d, numRows: %d\n", numCols, numRows);
    if(numCols < 3) {
        printf("number of columns needs to be atleast 3\n");
        return;
    }
    double *col1DataP = &dataP[0],
           *col2DataP = &dataP[numRows],
           *col3DataP = &dataP[2*numRows];
    for(int j = 0;j < numRows;j++) {
        col3DataP[j] = 2*col1DataP[j] + 3*col2DataP[j];
    }
    munmap(addr, sb.st_size);
    close(fd);
}

void
modifyColumnarData_io(const char *fileName) {
    printf("Modifying data using regular i/o\n");
    int fd = open(fileName, O_RDWR);
    if (fd == -1)
        handle_error("open read");
    struct stat sb;
    if (fstat(fd, &sb) == -1)           /* To obtain file size */
        handle_error("fstat read");

    ColumnarMetadata metaData;
    size_t bytesRead = read(fd, &metaData, sizeof(ColumnarMetadata));
    if(bytesRead < 0)
        handle_error("modify file read");
    int numCols = metaData.numCols, numRows = metaData.numRows;
    printf("numCols: %d, numRows: %d\n", numCols, numRows);
    if(numCols < 3) {
        printf("number of columns needs to be atleast 3\n");
        return;
    }
    double *col1DataP = (double *) malloc(numRows*sizeof(double)),
           *col2DataP = (double *) malloc(numRows*sizeof(double)),
           *col3DataP = (double *) malloc(numRows*sizeof(double));
    bytesRead = read(fd, col1DataP, numRows*sizeof(double));
    if(bytesRead < 0)
        handle_error("modify file read");
    bytesRead = read(fd, col2DataP, numRows*sizeof(double));
    if(bytesRead < 0)
        handle_error("modify file read");
    for(int j = 0;j < numRows;j++) {
        col3DataP[j] = 2*col1DataP[j] + 3*col2DataP[j];
    }

    //if (lseek (fd, sizeof(ColumnarMetadata)+(2*numRows*sizeof(double)), SEEK_SET) == -1)
    //    handle_error ("lseek error");

    size_t bytesWritten = write(fd, col3DataP, numRows*sizeof(double));
    if(bytesWritten < 0)
        handle_error("modify file write");

    free(col1DataP);
    free(col2DataP);
    free(col3DataP);
    close(fd);
}

void
readColumnarData(const char *fileName, int maxNumRows) {
    printf("reading data using mmap\n");
    int fd = open(fileName, O_RDONLY);
    if (fd == -1)
        handle_error("open read");
    struct stat sb;
    if (fstat(fd, &sb) == -1)           /* To obtain file size */
        handle_error("fstat read");
    char *addr = (char *) mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED)
        handle_error("mmap read");
    ColumnarData *dataMetadataP = (ColumnarData *) addr;
    double *dataP = (double *) dataMetadataP->data;
	int numCols = dataMetadataP->metaData.numCols, numRows = dataMetadataP->metaData.numRows;
	printf("numCols: %d, numRows: %d\n", numCols, numRows);
    maxNumRows = maxNumRows < numRows ? maxNumRows : numRows;
    for(int i = 0;i < numCols;i++) {
		double *colDataP = &dataP[i*numRows];
		for(int j = 0;j < maxNumRows;j++) {
			printf("%.1lf,", colDataP[j]);
		}
		printf("\n");
    }
    munmap(addr, sb.st_size);
    close(fd);
}

int
main(int argc, char *argv[])
{
    // create counters
    if(argc > 1 && strcmp(argv[1], "-counters") == 0) {
        writeCounters("counters.dat", 64*1024*1024);
        readCounters("counters.dat", 20);
    }
    if(argc > 1 && strcmp(argv[1], "-createColumnar") == 0) {
        writeColumnarData("columnarData.dat", 4, 16777216); // 67108864 on linux to get a 2GB file
    }
    if(argc > 1 && strcmp(argv[1], "-regularIO") == 0) {
        modifyColumnarData_io("columnarData.dat");
    }
    else {
        modifyColumnarData("columnarData.dat");
    }
    readColumnarData("columnarData.dat", 10);

#if 0
    char *addr;
    int fd;
    struct stat sb;
    off_t offset, pa_offset;
    size_t length;
    ssize_t s;
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "%s file offset [length]\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    fd = open(argv[1], O_RDONLY);
    if (fd == -1)
        handle_error("open");
    if (fstat(fd, &sb) == -1)           /* To obtain file size */
        handle_error("fstat");
    offset = atoi(argv[2]);
    pa_offset = offset & ~(sysconf(_SC_PAGE_SIZE) - 1);
        /* offset for mmap() must be page aligned */
    if (offset >= sb.st_size) {
        fprintf(stderr, "offset is past end of file\n");
        exit(EXIT_FAILURE);
    }
    if (argc == 4) {
        length = atoi(argv[3]);
        if (offset + length > sb.st_size)
            length = sb.st_size - offset;
                /* Canaqt display bytes past end of file */
    } else {    /* No length arg ==> display to end of file */
        length = sb.st_size - offset;
    }
    printf("offset %d, adjusted offset %d, pagesize %d\n", offset, pa_offset,
           sysconf(_SC_PAGE_SIZE));

    addr = (char *) mmap(NULL, length + offset - pa_offset, PROT_READ,
                MAP_PRIVATE, fd, pa_offset);
    if (addr == MAP_FAILED)
        handle_error("mmap");
    s = write(STDOUT_FILENO, addr + offset - pa_offset, length);
    if (s != length) {
        if (s == -1)
            handle_error("write");
        fprintf(stderr, "partial write");
        exit(EXIT_FAILURE);
    }
#endif

    exit(EXIT_SUCCESS);
}
