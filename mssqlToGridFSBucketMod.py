import pymssql
from pymongo import MongoClient
import gridfs
import time

def uploadResultFileAttach(mssqlConn, fsBucket):
    # Fetch records from mssql
    print("\nWorking on  Table\n")
    query = 'SELECT ResultFileAttachId, ResultId, FileTypeId, CreatedDateTime, FileName, FileContent from custSchema.ResultFileAttach'
    cursor = mssqlConn.cursor()
    cursor.execute(query)  

    row = cursor.fetchone()  
    insertCount = 0

    start = time.time()
    while row:  
        # insert records into MDB GridFS
        gridfsFile = fsBucket.upload_from_stream(filename=row[4], 
                                        source=row[5], 
                                        metadata={"createdDateTime": row[3], 
                                                    "fileTypeId":row[2], 
                                                    "resultId":row[1], 
                                                    "resultFileAttachId":row[0]})
        
        insertCount += 1
        if not (insertCount%50):
            print(f"\tInserted count - {insertCount}")

        # fetch next row from mssql and repeat
        row = cursor.fetchone()  

    end = time.time()
    cursor.close()

    total_time = end - start
    print(f"\nFinished - ResultFileAttach Table")
    print(f"\tTime Taken : {str(total_time)} \n")

def uploadResultReport(mssqlConn, fsBucket):
    # Fetch records from mssql

    print("----\nWorking on ResultReport Table")

    cursor = mssqlConn.cursor()
    cursor.execute('SELECT ResultReportId, ReportType, ReportText, createdDateTime from custSchema.ResultReport')  

    row = cursor.fetchone() 
    insertCount = 0 

    start = time.time()
    while row:  
        # insert records into MDB GridFS
        gridfsFile = fsBucket.put( row[2].encode('utf-8'),  createdDateTime=row[3] ,  resultReportId=row[0], filename=row[0] )

        insertCount += 1
        if not (insertCount%50):
            print(f"\tInserted count - {insertCount}")

        break
        # fetch next row from mssql and repeat
        row = cursor.fetchone()  

    end = time.time()
    cursor.close()

    total_time = end - start
    print("\nFinished - ResultReport Table")
    print(f"\tTime Taken : {str(total_time)} \n")

def queryGridFS(fs, mongoQuery):
    # read the inserted data from MongoDB -- only for verification
    for grid_data in fs.find(mongoQuery, no_cursor_timeout=True):
        data = grid_data.read()
        print(data)


if __name__ == "__main__":
    # connect  to MSSQL
    mssqlConn = pymssql.connect(server='localhost', user='sa', password='Password1', database='testdb', autocommit=True)  

    # connect to MongoDB
    mdbClient = MongoClient('mongodb+srv://custUser:custpwd@custtest.lgbev.mongodb.net')
    db = mdbClient.custBinary
    # get gridFSBucket
    custBucket = gridfs.GridFSBucket(db, "custGridBucket")   # instance of GridFSBucket class
    fsBucket = gridfs.GridFS(db)      # instance of GridFS class

    # insert data into GridFS
    uploadResultFileAttach(mssqlConn, custBucket)  # uses upload_from_stream API call
    uploadResultReport(mssqlConn, fsBucket)  # << uses different API call (put) to insert metadata at root level
    # However, note that in the above case, the collection names are - fs.files and fs.chunks 
                       
    # query data from GridFS
    mongoQuery1 = {"metadata.resultReportId": 115294374}
    queryGridFS(custBucket, mongoQuery1)   #   <<<<<<<  query from custGridBucket 
    mongoQuery2 = {"resultReportId": 115294374}     # <<<<< metadata at root level
    queryGridFS(fsBucket, mongoQuery2)   #   <<<<<<<  query from fs bucket



    mssqlConn.close()
    mdbClient.close()