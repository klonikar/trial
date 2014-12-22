// win_blob_io_oracle.cpp : Defines the entry point for the console application.
// Praneet Sharma
// run this once:
// set ORA_HOME=C:\app\klonikar\product\11.2.0\client_1\oci
// "\Program Files (x86)\Microsoft Visual Studio 9.0"\Common7\Tools\vsvars32.bat
// cl /nologo /EHsc -I %ORA_HOME%\include win_blob_io_oracle.cpp %ORA_HOME%\lib\msvc\oci.lib
#include <windows.h>
#include <oratypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <oci.h>
#include <iostream>
#include <malloc.h>
#include <string>

static text *username = (text *) "system";
static text *password = (text *) "praneet";

static OCIEnv           *p_env;
static OCIError         *p_err;
static OCISvcCtx        *p_svc;
static OCIStmt          *p_sql;
static OCIDefine        *p_dfn    = (OCIDefine *) 0;
static OCIBind          *p_bnd    = (OCIBind *) 0;
static OCILobLocator    *clob;

#define MAXBUFLEN  1048576//5000//1048576//65536//5000

typedef struct {
    int numCols;
    int numRows;
} ColumnarMetadata;

typedef struct {
      ColumnarMetadata metaData;
      char data[1]; // struct hack
} ColumnarData;

/* ----------------------------------------------------------------- */
/* Read operating system files into local buffers and then write the */
/* buffers to lobs using stream mode.                                */
/* ----------------------------------------------------------------- */

typedef struct __BLOBData {
      int len;
      char data[1];
} BLOBData;

int streamReadLobFromDBToFF(OCILobLocator *lobl, ub4 len, TCHAR * filename, int numRows)
{

      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);

  TCHAR * lpcTheFile = filename;
  size_t fileLength = len;
  //printf(("File has %d bytes\n"), fileLength);
  HANDLE hMapFile;   // handle for the file's memory-mapped region
  HANDLE hFile;      // file handle
  BOOL bFlag; 
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
      BLOBData *blobDataP = (BLOBData *) lpMapAddress;

      for(int i=0;i<numRows;i++)
      {
              ub4   offset = 1;
              ub4   loblen = 0;
              ub1   *bufp;
              ub4   amtp = 4096000000;
              sword retval;
              ub4   piece = 0;
              ub4   remainder;

              OCILobGetLength(p_svc, p_err, lobl, &loblen);
              //printf("To stream read LOB, loblen = %d\n", loblen);
              blobDataP->len = loblen;
              bufp = (ub1 *) blobDataP->data;

              retval = OCILobRead(p_svc, p_err, lobl, &amtp, offset, (dvoid *) bufp,
                                          (loblen < MAXBUFLEN ? loblen : MAXBUFLEN), (dvoid *)0, 
                                           (sb4 (*)(dvoid *, const dvoid *, ub4, ub1)) 0,
                                          (ub2) 0, (ub1) SQLCS_IMPLICIT);

              //printf("Previous amtp = 4gb, new amtp = %u\n", amtp);

              switch(retval)
              {
                  case OCI_SUCCESS:  /*Single piece of data*/
                        break;
                  case OCI_ERROR:
                        printf("Error in lobread\n");
                        break;
                  case OCI_NEED_DATA:   /*Streamed version of reading data*/
                        remainder = loblen;
                        bufp += MAXBUFLEN;
                        do
                        {
                              remainder = remainder - MAXBUFLEN;
                              retval = OCILobRead(p_svc, p_err, lobl, &amtp, offset, (dvoid *) bufp,
                                          (ub4) MAXBUFLEN, (dvoid *)0, 
                                           (sb4 (*)(dvoid *, const dvoid *, ub4, ub1)) 0,
                                          (ub2) 0, (ub1) SQLCS_IMPLICIT);
                              if (remainder < MAXBUFLEN) /*Last data chunk for a ROW*/
                              {
                                    bufp += remainder;
                              }
                              else
                              {
                                    bufp += MAXBUFLEN;
                              }
                        }while (retval == OCI_NEED_DATA);
                        break;
              } // end switch
              blobDataP = (BLOBData *) bufp;
      } // end for(rowNum)

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
      std::cout << "Elapsed time - " << elapsedTime << "ms\n";
    return 0;
}

int streamReadLobFromDBToFF_regulario(OCILobLocator *lobl, ub4 len, TCHAR * fileName, int numRows)
{
      LARGE_INTEGER frequency;        // ticks per second
      LARGE_INTEGER t1, t2;           // ticks
      static double elapsedTime = 0;
      static long long int timeCounter = 1;

      QueryPerformanceFrequency(&frequency);
      QueryPerformanceCounter(&t1);

    TCHAR * lpcTheFile = fileName;
      HANDLE hFile;        // file handle
      
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

      for(int i=0;i<numRows;i++)
      {
              ub4   offset = 1;
              ub4   loblen = 0;
              ub1   *bufp = (ub1 *)malloc(MAXBUFLEN);
              //ub1     bufp[MAXBUFLEN];  
              ub4   amtp = 4096000000;
              sword retval;
              ub4   piece = 0;
              ub4   remainder;
              DWORD numBytesRead = 0;

              OCILobGetLength(p_svc, p_err, lobl, &loblen);
              //printf("To stream read LOB, loblen = %d\n", loblen);

              //memset(bufp, '\0', MAXBUFLEN);

              retval = OCILobRead(p_svc, p_err, lobl, &amtp, offset, (dvoid *) bufp,
                                          (loblen < MAXBUFLEN ? loblen : MAXBUFLEN), (dvoid *)0, 
                                           (sb4 (*)(dvoid *, const dvoid *, ub4, ub1)) 0,
                                          (ub2) 0, (ub1) SQLCS_IMPLICIT);
            
              //printf("Previous amtp = 4gb, new amtp = %u\n", amtp);

              switch(retval)
              {
                  case OCI_SUCCESS:  /*Single piece of data*/
                        
                        break;
                  case OCI_ERROR:
                        printf("Error in lobread\n");
                        break;
                  case OCI_NEED_DATA:   /*Streamed version of reading data*/
                        remainder = loblen;
                        WriteFile(hFile, bufp, MAXBUFLEN, &numBytesRead, NULL);
                        do
                        {
                              //memset(bufp, '\0', MAXBUFLEN);
                              remainder = remainder - MAXBUFLEN;
                              retval = OCILobRead(p_svc, p_err, lobl, &amtp, offset, (dvoid *) bufp,
                                          (ub4) MAXBUFLEN, (dvoid *)0, 
                                           (sb4 (*)(dvoid *, const dvoid *, ub4, ub1)) 0,
                                          (ub2) 0, (ub1) SQLCS_IMPLICIT);
                              if (remainder < MAXBUFLEN) /*Last data chunk for a ROW*/
                              {
                                    WriteFile(hFile, bufp, remainder, &numBytesRead, NULL);
                              }
                              else
                              {
                                    WriteFile(hFile, bufp, MAXBUFLEN, &numBytesRead, NULL);
                              }
                        }while (retval == OCI_NEED_DATA);
                        break;      

              }
              free(bufp);
      }

      BOOL bFlag = CloseHandle(hFile);   // close the file itself

      if(!bFlag)
      {
            printf(("\nError %ld occurred closing the file!"),
            GetLastError());
      }

      QueryPerformanceCounter(&t2);
      elapsedTime = elapsedTime + (t2.QuadPart - t1.QuadPart) * 1000.0 / frequency.QuadPart;
      std::cout << "Elapsed time - " << elapsedTime << "ms\n";

    return 0;
}


void streamWriteLob(OCILobLocator *lobl, ub4 len, OraText * data)
{
  ub4   offset = 1;
  ub4   loblen = 0;
  ub1   * bufp = (ub1 *) data;
  ub4   amtp = len;
  sword retval;
  int   readval;
  ub4   len2 = 0;
  ub4   nbytes = (len > MAXBUFLEN ? MAXBUFLEN : len);
  ub4   remainder = len;
  ub1   piece = OCI_FIRST_PIECE;

  do
  {
      retval = OCILobWrite(p_svc, p_err, lobl, &amtp, offset, (dvoid *) bufp,
                          (ub4) nbytes, piece, (dvoid *)0, 
                          (sb4 (*)(dvoid *, dvoid *, ub4 *, ub1 *)) 0,
                          (ub2) 0, (ub1) SQLCS_IMPLICIT);

        remainder -= nbytes; 
        piece = OCI_NEXT_PIECE;
        bufp += nbytes;

        if (remainder > MAXBUFLEN)
        {
          nbytes = MAXBUFLEN;
        }
      else
      {
          nbytes = remainder;
          piece = OCI_LAST_PIECE;
      }

  } while (retval == OCI_NEED_DATA);
}


int main(int argc, char* argv[])
{
        int             p_bvi;
        char            p_sli[20];
        int             rc;
        OraText        errbuf[100];
        int             errcode;
        OraText *pSelStmt = (OraText *) "select username from all_users";
        ub2 dataLen = 0;
        sb2 dataInd = 0;
        printf("size - %d\n", sizeof(ub1));
        rc = OCIInitialize((ub4) OCI_DEFAULT, (dvoid *)0,  /* Initialize OCI */
                    (dvoid * (*)(dvoid *, size_t)) 0,
                    (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                    (void (*)(dvoid *, dvoid *)) 0 );

        /* Initialize evironment */
        //rc = OCIEnvCreate( (OCIEnv **) &p_env, OCI_DEFAULT, (void *) 0, 0, 0, 0,0,0 );
        rc = OCIEnvInit( (OCIEnv **) &p_env, OCI_DEFAULT, (size_t) 0, (dvoid **) 0 );
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Error - %.*sn", 512, errbuf);
            //exit(8);
        }
        /* Initialize handles */
        rc = OCIHandleAlloc( (dvoid *) p_env, (dvoid **) &p_err, OCI_HTYPE_ERROR,
                    (size_t) 0, (dvoid **) 0);
        rc = OCIHandleAlloc( (dvoid *) p_env, (dvoid **) &p_svc, OCI_HTYPE_SVCCTX,
                    (size_t) 0, (dvoid **) 0);
        rc = OCIDescriptorAlloc((dvoid *) p_env, (dvoid **) &clob, 
                         (ub4)OCI_DTYPE_LOB, (size_t) 0, (dvoid **) 0);
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Error - %.*sn", 512, errbuf);
            //exit(8);
        }
            
        /* Connect to database server */
        rc = OCILogon(p_env, p_err, &p_svc, (OraText *)username, 6, (OraText*)password, 8, (OraText*)"XE", 2);
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Error - %.*sn", 512, errbuf);
            //exit(8);
        }
                  
        
        /* Allocate and prepare SQL statement */
        rc = OCIHandleAlloc( (dvoid *) p_env, (dvoid **) &p_sql,
                    OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
        rc = OCIStmtPrepare(p_sql, p_err, (OraText *) pSelStmt,
                    (ub4) strlen((const char *)pSelStmt), (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);

        /* Define the select list items */
        rc = OCIDefineByPos(p_sql, &p_dfn, p_err, 1, (dvoid *) &p_sli,
          (sword) 20, SQLT_CHR, (dvoid *) &dataInd, (ub2 *)&dataLen,
          (ub2 *)0, OCI_DEFAULT);

        
        /* Execute the SQL statment */
        rc = OCIStmtExecute(p_svc, p_sql, p_err, (ub4) 1, (ub4) 0,
          (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT);
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Error - %.*sn", 512, errbuf);
            int justAnything2;
            std::cin >> justAnything2;
            //exit(8);
        }

        while (rc != OCI_NO_DATA) {             /* Fetch the remaining data */
              if(dataLen >= 0 && dataInd == 0) {
                  p_sli[dataLen] = 0; // null terminate string
                  printf("%s, %d, %d\n",p_sli, dataLen, dataInd);
                  rc = OCIStmtFetch(p_sql, p_err, 1, 0, 0);
              }
              else {
                    printf("dataInd: %d\n", dataInd);
              }
        }

        //DROP TABLE WITH CLOB COLUMN
        OraText * dropStmt = (OraText *) "DROP TABLE sampleClob";  //fld2 acts as ROW-INDEX to CLOB ROWS
        rc = OCIHandleAlloc( (dvoid *) p_env, (dvoid **) &p_sql,
                    OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
        rc = OCIStmtPrepare(p_sql, p_err, (OraText *) dropStmt,
                    (ub4) strlen((const char *)dropStmt), (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
        rc = OCIStmtExecute(p_svc, p_sql, p_err, (ub4) 1, (ub4) 0,
          (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT);
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Table doesn't exist for Deletion.\n");
        }

        //CREATING TABLE WITH CLOB COLUMN
        OraText * createStmt = (OraText *) "CREATE TABLE sampleClob(fld1 clob, fld2 INTEGER)";  //fld2 acts as ROW-INDEX to CLOB ROWS
        rc = OCIHandleAlloc( (dvoid *) p_env, (dvoid **) &p_sql,
                    OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
        rc = OCIStmtPrepare(p_sql, p_err, (OraText *) createStmt,
                    (ub4) strlen((const char *)createStmt), (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
        rc = OCIStmtExecute(p_svc, p_sql, p_err, (ub4) 1, (ub4) 0,
          (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT);
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Error - %.*sn", 512, errbuf);
            int justAnything2;
            std::cin >> justAnything2;
        }

        //DATA CREATION
        int rowSize = 10485760; //10000000;//8000; //10000000; //10 million
        char * data = (char *) malloc((rowSize+1) *sizeof(char));
        for(int i=0;i<rowSize;i++)
        {
              data[i] = 'B';
        }
        data[rowSize] = '\0'; 
        char * dataC = data;
        //std::cout << dataC << "\n";

        //std::string dataStr(data);
        //std::string str = "INSERT INTO sampleClob(fld1) VALUES('"+dataStr+"')";
        //std::cout << str << "\n";

        //INSERT 40 MILLION ROWS in TABLE
        //const char * d = str.c_str();
        //OraText * insertStmt = (OraText *) d;
        /*OraText * insertStmt = (OraText *)"INSERT INTO sampleCLob(fld1) VALUES(:1)";

        int maxRowNum = 1;
        rc = OCIHandleAlloc( (dvoid *) p_env, (dvoid **) &p_sql,
                          OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
        rc = OCIStmtPrepare(p_sql, p_err, (OraText *) insertStmt,
                    (ub4) strlen((const char *)insertStmt), (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Error - %.*sn", 512, errbuf);
            int justAnything2;
            std::cin >> justAnything2;
        }
        printf("Insert statement length is %d and rowSize is %d\n", strlen((const char *)insertStmt), rowSize*sizeof(char));
        rc = OCIBindByPos(p_sql, &p_bnd, p_err, 1, (OraText *)data, rowSize*sizeof(char),
                SQLT_CHR, 0, 0, 0, 0, 0, (ub4) OCI_DEFAULT);
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Error - %.*sn", 512, errbuf);
            int justAnything2;
            std::cin >> justAnything2;
        }
        for(int i=0;i<maxRowNum;i++)
        {
                    
                    rc = OCIStmtExecute(p_svc, p_sql, p_err, (ub4) 1, (ub4) 0,
                          (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT);
                    if (rc != 0) {
                        OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
                        printf("Error - %.*sn", 512, errbuf);
                        int justAnything2;
                        std::cin >> justAnything2;
                    }   
        }*/
        int rowNum = 50;
        int rowId;
        OraText * insertStmt = (OraText *)"INSERT INTO sampleCLob(fld1, fld2) VALUES(EMPTY_CLOB(), :1)";

        rc = OCIHandleAlloc( (dvoid *) p_env, (dvoid **) &p_sql,
                          OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
        rc = OCIStmtPrepare(p_sql, p_err, (OraText *) insertStmt,
                    (ub4) strlen((const char *)insertStmt), (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Error - %.*sn", 512, errbuf);
            int justAnything2;
            std::cin >> justAnything2;
        }

        printf("Insert prepared-statement length is %d and rowSize is %d\n", strlen((const char *)insertStmt), rowSize*sizeof(char));
        rc = OCIBindByPos(p_sql, &p_bnd, p_err, (ub4) 1,
                      (dvoid *) &rowId, (sb4) sizeof(rowId), SQLT_INT,
                      (dvoid *) 0, (ub2 *)0, (ub2 *)0,
                      (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
        if (rc != 0) {
            OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("Error - %.*sn", 512, errbuf);
            int justAnything2;
            std::cin >> justAnything2;
        }
        for(rowId =0;rowId<rowNum;rowId++)
        {
              rc = OCIStmtExecute(p_svc, p_sql, p_err, (ub4) 1, (ub4) 0,
                          (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT);
              if (rc != 0) {
                  OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
                  printf("Error - %.*sn", 512, errbuf);
                  int justAnything2;
                  std::cin >> justAnything2;
              }   
        }
        OCITransCommit(p_svc, p_err, (ub4)0);

        printf("Inserting empty_clob done \n");
        //SELECT CLOB LOCATOR FROM CLOB COLUMN
        for(int i=0;i<rowNum;i++)
        {
              int rowIdLocal = i;
              OraText * sqlStmt = (OraText *)"SELECT fld1 from sampleClob where fld2 = :1 FOR UPDATE";
              rc = OCIStmtPrepare(p_sql, p_err, sqlStmt, (ub4) strlen((char *)sqlStmt),
                                    (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
              if (rc != 0) {
                  OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
                  printf("Error - %.*sn", 512, errbuf);
                  int justAnything2;
                  std::cin >> justAnything2;
              }   

              rc = OCIBindByPos(p_sql, &p_bnd, p_err, (ub4) 1,
                      (dvoid *) &rowIdLocal, (sb4) sizeof(rowIdLocal), SQLT_INT,
                      (dvoid *) 0, (ub2 *)0, (ub2 *)0,
                      (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
              if (rc != 0) {
                  OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
                  printf("Error - %.*sn", 512, errbuf);
                  int justAnything2;
                  std::cin >> justAnything2;
              }   

              rc = OCIDefineByPos(p_sql, &p_dfn, p_err, (ub4) 1,
                                    (dvoid *) &clob, (sb4) -1, (ub2) SQLT_CLOB,
                                    (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) OCI_DEFAULT);
              if (rc != 0) {
                  OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
                  printf("Error - %.*sn", 512, errbuf);
                  int justAnything2;
                  std::cin >> justAnything2;
              }   

              rc = OCIStmtExecute(p_svc, p_sql, p_err, (ub4) 1, (ub4) 0,
                                    (CONST OCISnapshot*) 0, (OCISnapshot*) 0,  
                                    (ub4) OCI_DEFAULT);
              if (rc != 0) {
                  OCIErrorGet((dvoid *)p_err, (ub4) 1, (text *) NULL, &errcode, (OraText *)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
                  printf("Error - %.*sn", 512, errbuf);
                  int justAnything2;
                  std::cin >> justAnything2;
              }   

              printf("Going to write data through Streaming\n");
        
              streamWriteLob(clob, rowSize, (OraText *)data);
        }
        //streamReadLobFromDBToFF(clob, rowNum*rowSize+sizeof(int)*rowNum, TEXT("fileMapping_DBtoFF.txt"), rowNum);
        streamReadLobFromDBToFF_regulario(clob, rowNum*rowSize, TEXT("fileMapping_DBtoFF_regulario.txt"), rowNum);
        rc = OCILogoff(p_svc, p_err);                           /* Disconnect */
        rc = OCIHandleFree((dvoid *) p_sql, OCI_HTYPE_STMT);    /* Free handles */
        rc = OCIHandleFree((dvoid *) p_svc, OCI_HTYPE_SVCCTX);
        rc = OCIHandleFree((dvoid *) p_err, OCI_HTYPE_ERROR);
        rc = OCIDescriptorFree((dvoid *) clob, (ub4) OCI_DTYPE_LOB);
        printf("Done \n");
        int justAnything;
        std::cin >> justAnything;
        return 0;
}
