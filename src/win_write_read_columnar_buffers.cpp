// win_write_read_columnar_buffers.cpp : Defines the entry point for the console application.
// Praneet Sharma

//#include "stdafx.h"
#include<iostream>

using namespace std;

#include <windows.h>
#include <stdio.h>
#include <conio.h>
#include <tchar.h>
#include <ctime>


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

      _tprintf(TEXT("File has %d bytes\n"), fileLength);

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
        _tprintf(TEXT("hFile is NULL\n"));
        _tprintf(TEXT("Target file is %s\n"),
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
            _tprintf(TEXT("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return 2;
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
            _tprintf(TEXT("lpMapAddress is NULL: last error: %d\n"), GetLastError());
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
            _tprintf(TEXT("\nError %ld occurred closing the mapping object!"),
            GetLastError());
			return -1;
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            _tprintf(TEXT("\nError %ld occurred closing the file!"),
            GetLastError());
			return -1;
      }


      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms" << endl;
      _tprintf(TEXT("File modification complete\n"));
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


      //_tprintf(TEXT("Modifying data using FileMapping on Windows\n"));

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
        _tprintf(TEXT("hFile is NULL\n"));
        _tprintf(TEXT("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }

      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      DWORD dwFileMapSize = dwFileSize;
      DWORD dwMapViewSize = dwFileSize;

      //_tprintf(TEXT("File size is %d\n"), dwFileSize);

      hMapFile = CreateFileMapping( hFile,          // current file handle
                NULL,           // default security
                PAGE_READWRITE, // read/write permission
                0,              // size of mapping object, high
                dwFileMapSize,  // size of mapping object, low
                NULL);  
      if (hMapFile == NULL)
      {
            _tprintf(TEXT("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return 2;
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
            _tprintf(TEXT("lpMapAddress is NULL: last error: %d\n"), GetLastError());
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
            _tprintf(TEXT("\nError %ld occurred closing the mapping object!"),
            GetLastError());
			return -1;
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            _tprintf(TEXT("\nError %ld occurred closing the file!"),
            GetLastError());
			return -1;
      }



      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms" << endl;
      _tprintf(TEXT("File modification complete\n"));
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

      //_tprintf(TEXT("Reading data using FileMapping on Windows\n"));

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
        _tprintf(TEXT("hFile is NULL\n"));
        _tprintf(TEXT("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }

      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      DWORD dwFileMapSize = dwFileSize;
      DWORD dwMapViewSize = dwFileSize;

      //_tprintf(TEXT("File size is %d\n"), dwFileSize);

      hMapFile = CreateFileMapping( hFile,          // current file handle
                NULL,           // default security
                PAGE_READWRITE, // read/write permission
                0,              // size of mapping object, high
                dwFileMapSize,  // size of mapping object, low
                NULL);  
      if (hMapFile == NULL)
      {
            _tprintf(TEXT("hMapFile is NULL: last error: %d\n"), GetLastError() );
            return 2;
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
            _tprintf(TEXT("lpMapAddress is NULL: last error: %d\n"), GetLastError());
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
            _tprintf(TEXT("\nError %ld occurred closing the mapping object!"),
            GetLastError());
			return -1;
      }

      bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            _tprintf(TEXT("\nError %ld occurred closing the file!"),
            GetLastError());
			return -1;
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

      //_tprintf(TEXT("Modifying file using regular IO on windows platform \n"));

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
        _tprintf(TEXT("hFile is NULL\n"));
        _tprintf(TEXT("Target file is %s\n"),
             lpcTheFile);
        return 4;
    }
      
      DWORD dwFileSize = GetFileSize(hFile,  NULL);
      //_tprintf(TEXT("Number of bytes to read - %d\n"),dwFileSize);

      ColumnarMetadata metaData;
      LPVOID buf = NULL;
      DWORD numBytesRead = 0;
      BOOL isRead = ReadFile(hFile, &metaData, sizeof(ColumnarMetadata), &numBytesRead, NULL);

      //_tprintf(TEXT("Number of bytes read %d\n"), numBytesRead);
      //_tprintf(TEXT("is file read - %d\n"), isRead);

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

      //_tprintf(TEXT("Number of bytes written %d\n"), numBytesRead);

      BOOL bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            _tprintf(TEXT("\nError %ld occurred closing the file!"),
            GetLastError());
      }

      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms" << endl;
      _tprintf(TEXT("File modification complete\n"));
	  return 0;
}

int _tmain(int argc, _TCHAR* argv[])
{

      //writeColumnarData(TEXT("fmtest2.txt"), 3, 4000000); /*40000000*/
      //modifyColumnarData(TEXT("fmtest2.txt"));
      modifyColumnarData_regulario(TEXT("fmtest2.txt"));
      readColumnarData(TEXT("fmtest2.txt"), 10);
      
      int sampleOutput;
      cin >> sampleOutput;
      return 0;
}