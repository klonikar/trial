// win_write_read_columnar_buffers.cpp : Defines the entry point for the console application.
// Praneet Sharma
// cl /nologo /EHsc win_write_read_columnar_buffers.cpp
#include <iostream>
#include <windows.h>
#include <stdio.h>
#include <conio.h>
#include <ctime>

using namespace std;

typedef struct {
    int numCols;
    int numRows;
} ColumnarMetadata;

typedef struct {
      ColumnarMetadata metaData;
      char data[1]; // struct hack
} ColumnarData;

int writeColumnarData(TCHAR * filename, int numCols, int numRows)
{
      TCHAR * lpcTheFile = filename;
      
      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);

      size_t fileLength = sizeof(ColumnarMetadata) + numCols*numRows*sizeof(double); /*4*numCols*numRows;*/

      printf(("File has %d bytes\n"), fileLength);

      HANDLE hMapFile;   // handle for the file's memory-mapped region
      HANDLE hFile;        // file handle
      BOOL bFlag;        // a result holder

      DWORD dwFileMapStart = 0; //start file map view from 0
      DWORD dwFileMapSize = fileLength;
      DWORD dwMapViewSize = fileLength;
      LPVOID lpMapAddress;

      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     CREATE_ALWAYS,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);

      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }

      

      hMapFile = CreateFileMapping( hFile,          // current file handle
                NULL,           // default security
                PAGE_READWRITE, // read/write permission
                0,              // size of mapping object, high
                dwFileMapSize,  // size of mapping object, low
                NULL);  
      if (hMapFile == NULL)
      {
            printf(("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return (2);
      }



      lpMapAddress = MapViewOfFile(hMapFile,            // handle to
                                                    // mapping object
                               FILE_MAP_ALL_ACCESS, // read/write
                               0,                   // high-order 32
                                                    // bits of file
                                                    // offset
                               dwFileMapStart,      // low-order 32
                                                    // bits of file
                                                    // offset
                               dwMapViewSize);      // number of bytes
                                                    // to map
      if (lpMapAddress == NULL)
      {
            printf(("lpMapAddress is NULL: last error: %d\n"), GetLastError());
            return 3;
      }


      ColumnarData *dataMetadataP = (ColumnarData *) lpMapAddress;
    double *dataP = (double *) dataMetadataP->data;
    // Set data
      dataMetadataP->metaData.numCols = numCols;
      dataMetadataP->metaData.numRows = numRows;
    for(int i = 0;i < numCols;i++) 
      {
            double *colDataP = &dataP[i*numRows];
            for(int j = 0;j < numRows;j++) 
            {
                  colDataP[j] = (i+1)*(j+1);
          }
    }




      // Close the file mapping object and the open file

      bFlag = UnmapViewOfFile(lpMapAddress);
      bFlag = CloseHandle(hMapFile); // close the file mapping object

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the mapping object!"),
            GetLastError());
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }


      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms" << endl;
      printf(("File modification complete\n"));

    return 0;
}







int writeColumnarData_rowMajor(TCHAR * filename, int numCols, int numRows)
{
      TCHAR * lpcTheFile = filename;
      
      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);

      size_t fileLength = sizeof(ColumnarMetadata) + numCols*numRows*sizeof(double); /*4*numCols*numRows;*/

      printf(("File has %d bytes\n"), fileLength);

      HANDLE hMapFile;   // handle for the file's memory-mapped region
      HANDLE hFile;        // file handle
      BOOL bFlag;        // a result holder

      DWORD dwFileMapStart = 0; //start file map view from 0
      DWORD dwFileMapSize = fileLength;
      DWORD dwMapViewSize = fileLength;
      LPVOID lpMapAddress;

      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     CREATE_ALWAYS,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);

      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }

      

      hMapFile = CreateFileMapping( hFile,          // current file handle
                NULL,           // default security
                PAGE_READWRITE, // read/write permission
                0,              // size of mapping object, high
                dwFileMapSize,  // size of mapping object, low
                NULL);  
      if (hMapFile == NULL)
      {
            printf(("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return (2);
      }



      lpMapAddress = MapViewOfFile(hMapFile,            // handle to
                                                    // mapping object
                               FILE_MAP_ALL_ACCESS, // read/write
                               0,                   // high-order 32
                                                    // bits of file
                                                    // offset
                               dwFileMapStart,      // low-order 32
                                                    // bits of file
                                                    // offset
                               dwMapViewSize);      // number of bytes
                                                    // to map
      if (lpMapAddress == NULL)
      {
            printf(("lpMapAddress is NULL: last error: %d\n"), GetLastError());
            return 3;
      }


      ColumnarData *dataMetadataP = (ColumnarData *) lpMapAddress;
    double *dataP = (double *) dataMetadataP->data;
    // Set data
      dataMetadataP->metaData.numCols = numCols;
      dataMetadataP->metaData.numRows = numRows;
      
      // Filling FlatFile Row by Row
      for(int i=0; i<numRows; i++)
      {
            double *rowDataP = &dataP[i*numCols];
            for(int j=0; j<numCols; j++)
            {
                  rowDataP[j] = (i+1)*(j+1);
            }
      }


      // Close the file mapping object and the open file

      bFlag = UnmapViewOfFile(lpMapAddress);
      bFlag = CloseHandle(hMapFile); // close the file mapping object

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the mapping object!"),
            GetLastError());
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }


      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms" << endl;
      printf(("File modification complete\n"));

    return 0;
}

int modifyColumnarData(TCHAR * filename)
{
      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);


      //printf(("Modifying data using FileMapping on Windows\n"));

      TCHAR * lpcTheFile = filename;

      HANDLE hFile;        // file handle
      HANDLE hMapFile;   // handle for the file's memory-mapped region
      BOOL bFlag;        // a result holder
      
      DWORD dwFileMapStart = 0; //start file map view from 0
      LPVOID lpMapAddress;


      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);
      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }

      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      DWORD dwFileMapSize = dwFileSize;
      DWORD dwMapViewSize = dwFileSize;

      //printf(("File size is %d\n"), dwFileSize);

      hMapFile = CreateFileMapping( hFile,          // current file handle
                NULL,           // default security
                PAGE_READWRITE, // read/write permission
                0,              // size of mapping object, high
                dwFileMapSize,  // size of mapping object, low
                NULL);  
      if (hMapFile == NULL)
      {
            printf(("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return (2);
      }
      
      lpMapAddress = MapViewOfFile(hMapFile,            // handle to
                                                    // mapping object
                               FILE_MAP_ALL_ACCESS, // read/write
                               0,                   // high-order 32
                                                    // bits of file
                                                    // offset
                               dwFileMapStart,      // low-order 32
                                                    // bits of file
                                                    // offset
                               dwMapViewSize);      // number of bytes
                                                    // to map
      if (lpMapAddress == NULL)
      {
            printf(("lpMapAddress is NULL: last error: %d\n"), GetLastError());
            return 3;
      }

      

      ColumnarData *dataMetadataP = (ColumnarData *) lpMapAddress;
    double *dataP = (double *) dataMetadataP->data;
      int numCols = dataMetadataP->metaData.numCols, numRows = dataMetadataP->metaData.numRows;
      printf("numCols: %d, numRows: %d\n", numCols, numRows);
    if(numCols < 3) {
        printf("number of columns needs to be atleast 3\n");
        return 0;
    }

      double *col1DataP = &dataP[0],
           *col2DataP = &dataP[numRows],
           *col3DataP = &dataP[2*numRows];
    for(int j = 0;j < numRows;j++) {
        col3DataP[j] = 2*col1DataP[j] + 3*col2DataP[j];
    }

      //UNMAPPING

      bFlag = UnmapViewOfFile(lpMapAddress);
      bFlag = CloseHandle(hMapFile); // close the file mapping object

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the mapping object!"),
            GetLastError());
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }



      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms" << endl;
      printf(("File modification complete\n"));
    return 0;
}

int modifyColumnarData_rowMajor(TCHAR * filename)
{


      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);


      //printf(("Modifying data using FileMapping on Windows\n"));

      TCHAR * lpcTheFile = filename;

      HANDLE hFile;        // file handle
      HANDLE hMapFile;   // handle for the file's memory-mapped region
      BOOL bFlag;        // a result holder
      
      DWORD dwFileMapStart = 0; //start file map view from 0
      LPVOID lpMapAddress;


      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);
      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }

      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      DWORD dwFileMapSize = dwFileSize;
      DWORD dwMapViewSize = dwFileSize;

      //printf(("File size is %d\n"), dwFileSize);

      hMapFile = CreateFileMapping( hFile,          // current file handle
                NULL,           // default security
                PAGE_READWRITE, // read/write permission
                0,              // size of mapping object, high
                dwFileMapSize,  // size of mapping object, low
                NULL);  
      if (hMapFile == NULL)
      {
            printf(("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return (2);
      }
      
      lpMapAddress = MapViewOfFile(hMapFile,            // handle to
                                                    // mapping object
                               FILE_MAP_ALL_ACCESS, // read/write
                               0,                   // high-order 32
                                                    // bits of file
                                                    // offset
                               dwFileMapStart,      // low-order 32
                                                    // bits of file
                                                    // offset
                               dwMapViewSize);      // number of bytes
                                                    // to map
      if (lpMapAddress == NULL)
      {
            printf(("lpMapAddress is NULL: last error: %d\n"), GetLastError());
            return 3;
      }

      

      ColumnarData *dataMetadataP = (ColumnarData *) lpMapAddress;
    double *dataP = (double *) dataMetadataP->data;
      int numCols = dataMetadataP->metaData.numCols, numRows = dataMetadataP->metaData.numRows;
      printf("numCols: %d, numRows: %d\n", numCols, numRows);
    if(numCols < 3) {
        printf("number of columns needs to be atleast 3\n");
        return 0;
    }


      // Modifying 3rd column for every row
      for(int i=0; i<numRows; i++)
      {
            double *rowDataP = &dataP[i*numCols];
            for(int j=0; j<numCols; j++)
            {
                  rowDataP[2] = 2*rowDataP[0] + 3*rowDataP[1];
            }
      }


      //UNMAPPING

      bFlag = UnmapViewOfFile(lpMapAddress);
      bFlag = CloseHandle(hMapFile); // close the file mapping object

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the mapping object!"),
            GetLastError());
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }



      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms" << endl;
      printf(("File modification complete\n"));
    return 0;
}


int readColumnarData(TCHAR * filename, int maxNumRows)
{
      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);


      //printf(("Reading data using FileMapping on Windows\n"));

      TCHAR * lpcTheFile = filename;

      HANDLE hFile;        // file handle
      HANDLE hMapFile;   // handle for the file's memory-mapped region
      BOOL bFlag;        // a result holder
      
      DWORD dwFileMapStart = 0; //start file map view from 0
      LPVOID lpMapAddress;


      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);
      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }

      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      DWORD dwFileMapSize = dwFileSize;
      DWORD dwMapViewSize = dwFileSize;

      //printf(("File size is %d\n"), dwFileSize);

      hMapFile = CreateFileMapping( hFile,          // current file handle
                NULL,           // default security
                PAGE_READWRITE, // read/write permission
                0,              // size of mapping object, high
                dwFileMapSize,  // size of mapping object, low
                NULL);  
      if (hMapFile == NULL)
      {
            printf(("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return (2);
      }
      
      lpMapAddress = MapViewOfFile(hMapFile,            // handle to
                                                    // mapping object
                               FILE_MAP_ALL_ACCESS, // read/write
                               0,                   // high-order 32
                                                    // bits of file
                                                    // offset
                               dwFileMapStart,      // low-order 32
                                                    // bits of file
                                                    // offset
                               dwMapViewSize);      // number of bytes
                                                    // to map
      if (lpMapAddress == NULL)
      {
            printf(("lpMapAddress is NULL: last error: %d\n"), GetLastError());
            return 3;
      }

      ColumnarData *dataMetadataP = (ColumnarData *) lpMapAddress;
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


      //UNMAPPING

      bFlag = UnmapViewOfFile(lpMapAddress);
      bFlag = CloseHandle(hMapFile); // close the file mapping object

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the mapping object!"),
            GetLastError());
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }
    return 0;
}

int readColumnarData_rowMajor(TCHAR * filename, int maxNumRows)
{
      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);


      //printf(("Reading data using FileMapping on Windows\n"));

      TCHAR * lpcTheFile = filename;

      HANDLE hFile;        // file handle
      HANDLE hMapFile;   // handle for the file's memory-mapped region
      BOOL bFlag;        // a result holder
      
      DWORD dwFileMapStart = 0; //start file map view from 0
      LPVOID lpMapAddress;


      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);
      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }

      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      DWORD dwFileMapSize = dwFileSize;
      DWORD dwMapViewSize = dwFileSize;

      //printf(("File size is %d\n"), dwFileSize);

      hMapFile = CreateFileMapping( hFile,          // current file handle
                NULL,           // default security
                PAGE_READWRITE, // read/write permission
                0,              // size of mapping object, high
                dwFileMapSize,  // size of mapping object, low
                NULL);  
      if (hMapFile == NULL)
      {
            printf(("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return (2);
      }
      
      lpMapAddress = MapViewOfFile(hMapFile,            // handle to
                                                    // mapping object
                               FILE_MAP_ALL_ACCESS, // read/write
                               0,                   // high-order 32
                                                    // bits of file
                                                    // offset
                               dwFileMapStart,      // low-order 32
                                                    // bits of file
                                                    // offset
                               dwMapViewSize);      // number of bytes
                                                    // to map
      if (lpMapAddress == NULL)
      {
            printf(("lpMapAddress is NULL: last error: %d\n"), GetLastError());
            return 3;
      }

      ColumnarData *dataMetadataP = (ColumnarData *) lpMapAddress;
    double *dataP = (double *) dataMetadataP->data;
      int numCols = dataMetadataP->metaData.numCols, numRows = dataMetadataP->metaData.numRows;
      printf("numCols: %d, numRows: %d\n", numCols, numRows);
    maxNumRows = maxNumRows < numRows ? maxNumRows : numRows;

      // Modifying 3rd column for every row
      for(int i=0; i<maxNumRows; i++)
      {
            double *rowDataP = &dataP[i*numCols];
            for(int j=0; j<numCols; j++)
            {
                  printf("%.1lf,", rowDataP[j]);
            }
            printf("\n");
      }


    /*for(int i = 0;i < numCols;i++) {
            double *colDataP = &dataP[i*numRows];
            for(int j = 0;j < maxNumRows;j++) {
                  printf("%.1lf,", colDataP[j]);
            }
            printf("\n");
    }*/


      //UNMAPPING

      bFlag = UnmapViewOfFile(lpMapAddress);
      bFlag = CloseHandle(hMapFile); // close the file mapping object

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the mapping object!"),
            GetLastError());
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }
    return 0;
}

int modifyColumnarData_regulario(TCHAR *fileName)
{

      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);

      //printf(("Modifying file using regular IO on windows platform \n"));

      TCHAR * lpcTheFile = fileName;

      HANDLE hFile;        // file handle
      
      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);
      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }
      
      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      //printf(("Number of bytes to read - %d\n"),dwFileSize);

      ColumnarMetadata metaData;
      LPVOID buf = NULL;
      DWORD numBytesRead = 0;
      BOOL isRead = ReadFile(hFile, &metaData, sizeof(ColumnarMetadata), &numBytesRead, NULL);

      //printf(("Number of bytes read %d\n"), numBytesRead);
      //printf(("is file read - %d\n"), isRead);

      int numCols = metaData.numCols, numRows = metaData.numRows;
   // printf("numCols: %d, numRows: %d\n", numCols, numRows);
    if(numCols < 3) {
        printf("number of columns needs to be atleast 3\n");
        return 0;
    }

      double *col1DataP = (double *) malloc(numRows*sizeof(double)),
           *col2DataP = (double *) malloc(numRows*sizeof(double)),
           *col3DataP = (double *) malloc(numRows*sizeof(double));

      isRead = ReadFile(hFile, col1DataP, numRows*sizeof(double), &numBytesRead, NULL);

      isRead = ReadFile(hFile, col2DataP, numRows*sizeof(double), &numBytesRead, NULL);

      for(int j = 0;j < numRows;j++) {
        col3DataP[j] = 2*col1DataP[j] + 3*col2DataP[j];
    }

      WriteFile(hFile, col3DataP, numRows*sizeof(double), &numBytesRead, NULL);

      free(col1DataP);
      free(col2DataP);
      free(col3DataP);

      //printf(("Number of bytes written %d\n"), numBytesRead);

      

      BOOL bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }

      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms" << endl;
      printf(("File modification complete\n"));
    return 0;
}

int modifyColumnarData_regulario_rowMajor(TCHAR *fileName)
{

      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);

      //printf(("Modifying file using regular IO on windows platform \n"));

      TCHAR * lpcTheFile = fileName;

      HANDLE hFile;        // file handle
      
      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);
      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }
      
      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      //printf(("Number of bytes to read - %d\n"),dwFileSize);

      ColumnarMetadata metaData;
      LPVOID buf = NULL;
      DWORD numBytesRead = 0;
      BOOL isRead = ReadFile(hFile, &metaData, sizeof(ColumnarMetadata), &numBytesRead, NULL);

      //printf(("Number of bytes read %d\n"), numBytesRead);
      //printf(("is file read - %d\n"), isRead);

      int numCols = metaData.numCols, numRows = metaData.numRows;
   // printf("numCols: %d, numRows: %d\n", numCols, numRows);
    if(numCols < 3) {
        printf("number of columns needs to be atleast 3\n");
        return 0;
    }

      double *col1DataP = (double *) malloc(numRows*sizeof(double)),
           *col2DataP = (double *) malloc(numRows*sizeof(double)),
           *col3DataP = (double *) malloc(numRows*sizeof(double));

      isRead = ReadFile(hFile, col1DataP, numRows*sizeof(double), &numBytesRead, NULL);

      isRead = ReadFile(hFile, col2DataP, numRows*sizeof(double), &numBytesRead, NULL);

      for(int j = 0;j < numRows;j++) {
        col3DataP[j] = 2*col1DataP[j] + 3*col2DataP[j];
    }

      WriteFile(hFile, col3DataP, numRows*sizeof(double), &numBytesRead, NULL);

      free(col1DataP);
      free(col2DataP);
      free(col3DataP);

      //printf(("Number of bytes written %d\n"), numBytesRead);

      

      BOOL bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }

      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms" << endl;
      printf(("File modification complete\n"));
    return 0;
}

int
simulateRandomIO(const char *filename, int *startOffsets, int *bytesToModify, int maxTimes) {
      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);


      //printf(("Modifying data using FileMapping on Windows\n"));

      const TCHAR * lpcTheFile = filename;

      HANDLE hFile;        // file handle
      HANDLE hMapFile;   // handle for the file's memory-mapped region
      BOOL bFlag;        // a result holder
      
      DWORD dwFileMapStart = 0; //start file map view from 0
      LPVOID lpMapAddress;


      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);
      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }

      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      DWORD dwFileMapSize = dwFileSize;
      DWORD dwMapViewSize = dwFileSize;

      //printf(("File size is %d\n"), dwFileSize);

      hMapFile = CreateFileMapping( hFile,          // current file handle
                NULL,           // default security
                PAGE_READWRITE, // read/write permission
                0,              // size of mapping object, high
                dwFileMapSize,  // size of mapping object, low
                NULL);  
      if (hMapFile == NULL)
      {
            printf(("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return (2);
      }
      
      lpMapAddress = MapViewOfFile(hMapFile,            // handle to
                                                    // mapping object
                               FILE_MAP_ALL_ACCESS, // read/write
                               0,                   // high-order 32
                                                    // bits of file
                                                    // offset
                               dwFileMapStart,      // low-order 32
                                                    // bits of file
                                                    // offset
                               dwMapViewSize);      // number of bytes
                                                    // to map
      if (lpMapAddress == NULL)
      {
            printf(("lpMapAddress is NULL: last error: %d\n"), GetLastError());
            return 3;
      }

      
    char *addr = (char *) lpMapAddress;
	for(int i = 0;i < maxTimes;i++) {
		char *offsetPtr = addr + startOffsets[i];
		int numModify = bytesToModify[i];
		for(int j = 0;j < numModify;j++) {
			offsetPtr[j]++; // read the byte and modify it.
		}
	}

      //UNMAPPING

      bFlag = UnmapViewOfFile(lpMapAddress);
      bFlag = CloseHandle(hMapFile); // close the file mapping object

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the mapping object!"),
            GetLastError());
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }



      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "mmap Elapsed time - " << elapsedTime << "ms" << endl;
      printf(("File modification complete\n"));

	printf("modified %d times\nsample offsets: %d, %d, %d, %d\n", maxTimes, startOffsets[0], startOffsets[1], startOffsets[2], startOffsets[3]);
	printf("sample bytes: %d, %d, %d, %d\n", bytesToModify[0], bytesToModify[1], bytesToModify[2], bytesToModify[3]);

	  return 0;
}

int
simulateRandomIO_regularIO(const char *fileName, int *startOffsets, int *bytesToModify, int maxTimes, int maxBytesToModify) {

      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);

      //printf(("Modifying file using regular IO on windows platform \n"));

      const TCHAR * lpcTheFile = fileName;

      HANDLE hFile;        // file handle
      
      hFile = CreateFile(lpcTheFile,
                     GENERIC_READ | GENERIC_WRITE,
                     0,
                     NULL,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL,
                     NULL);
      if (hFile == INVALID_HANDLE_VALUE)
    {
        printf(("hFile is NULL\n"));
        printf(("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }
      
      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      //printf(("Number of bytes to read - %d\n"),dwFileSize);

    char *buff = (char *) malloc(maxBytesToModify);
    if (buff == NULL)
    {
        printf(("malloc failure %s\n"),
             lpcTheFile);
        return 4;
    }

	DWORD numBytesRead = 0;
	for(int i = 0;i < maxTimes;i++) {
		DWORD dwPtr = SetFilePointer(hFile, startOffsets[i], NULL, FILE_BEGIN);
		int numModify = bytesToModify[i];
		BOOL isRead = ReadFile(hFile, buff, numModify, &numBytesRead, NULL);
		for(int j = 0;j < numModify;j++) {
			buff[j]++; // modify it.
		}
		dwPtr = SetFilePointer(hFile, startOffsets[i], NULL, FILE_BEGIN);
		WriteFile(hFile, buff, numModify, &numBytesRead, NULL);
	}

      //printf(("Number of bytes written %d\n"), numBytesRead);

      

      BOOL bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }

      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "regular IO Elapsed time - " << elapsedTime << "ms" << endl;
      printf(("File modification complete\n"));

	printf("modified %d times\nsample offsets: %d, %d, %d, %d\n", maxTimes, startOffsets[0], startOffsets[1], startOffsets[2], startOffsets[3]);
	printf("sample bytes: %d, %d, %d, %d\n", bytesToModify[0], bytesToModify[1], bytesToModify[2], bytesToModify[3]);

    return 0;
}

int
myRand(int maxVal) {
    return (int) (maxVal * ((double) rand())/(RAND_MAX + 1.0));
}

int main(int argc, char* argv[])
{
	const int maxTimesToModify = 100000;
	const int maxOffset = 1024*1024; // it has to be file size
	const int maxBytesToModify = 4*1024 + 1; // 8K, 2 times page size
	int startOffsets[maxTimesToModify], bytesToModify[maxTimesToModify];
	srand(time(NULL));
	
	for(int i = 0;i < maxTimesToModify;i++) {
		startOffsets[i] = myRand(maxOffset);
		bytesToModify[i] = myRand(maxBytesToModify);
	}

      //writeColumnarData(("fmtest2.txt"), 3, 4000000); /*40000000*/
		simulateRandomIO("fmtest2.txt", startOffsets, bytesToModify, maxTimesToModify);
		simulateRandomIO_regularIO("fmtest2.txt", startOffsets, bytesToModify, maxTimesToModify, maxBytesToModify);

      //modifyColumnarData(("fmtest2.txt"));
      //modifyColumnarData_regulario(("fmtest2.txt"));
      //readColumnarData(("fmtest2.txt"), 10);

      //writeColumnarData_rowMajor(("fmtest2_rowMajor.txt"), 3, 40000000); /*40000000*/
      //modifyColumnarData_rowMajor(("fmtest2_rowMajor.txt"));
      //modifyColumnarData_regulario_rowMajor(("fmtest2_rowMajor.txt"));
      //readColumnarData_rowMajor(("fmtest2_rowMajor.txt"), 10);
      
	  cout << "hit any key and enter to continue" << endl;
      int sampleOutput;
      cin >> sampleOutput;
      return 0;
}
