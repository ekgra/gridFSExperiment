import pymssql
from pymongo import MongoClient
import gridfs
import time

# connect  to MSSQL
mssqlConn = pymssql.connect(server='localhost', user='sa', password='********', database='testdb', autocommit=True)  

# connect to MongoDB
mdbClient = MongoClient('mongodb+srv://custUser:********@custtest.lgbev.mongodb.net')
db = mdbClient.custBinary
fs = gridfs.GridFSBucket(db, "custGridBucket")



# Fetch records from mssql
print("\nWorking on resultFileAttach Table\n")
selectRFACursor = mssqlConn.cursor()
selectRFACursor.execute('SELECT ResultFileAttachId, ResultId, FileTypeId, CreatedDateTime, FileName, FileContent from custSchema.ResultFileAttach')  


row = selectRFACursor.fetchone()  
insertCount = 0

start = time.time()
while row:  
    # insert records into MDB GridFS
    gridfsFile = fs.upload_from_stream(filename=row[4], 
                                       source=row[5], 
                                       metadata={"createdDateTime": row[3], 
                                                 "fileTypeId":row[2], 
                                                 "resultId":row[1], 
                                                 "resultFileAttachId":row[0]})
    
    insertCount += 1
    if not (insertCount%50):
        print(f"\tInserted count - {insertCount}")
    
    # read the inserted data from MongoDB -- only for verification
    # for grid_data in fs.find({"metadata.resultFileAttachId": row[0]}, no_cursor_timeout=True):
    #     data = grid_data.read()
    #     print(data)

    print(type(row[5]))

    # fetch next row from mssql and repeat
    row = selectRFACursor.fetchone()  

end = time.time()
selectRFACursor.close()

total_time = end - start
print("\nFinished - ResultFileAttach Table")
print(f"\tTime Taken : {str(total_time)} \n")


print("----\nWorking on ResultReport Table")

selectRRCursor = mssqlConn.cursor()
selectRRCursor.execute('SELECT ResultReportId, ReportType, ReportText, createdDateTime from custSchema.ResultReport')  

row = selectRRCursor.fetchone() 
insertCount = 0 

start = time.time()
while row:  
    # insert records into MDB GridFS
    gridfsFile = fs.upload_from_stream(filename=str(row[0]), 
                                       source=row[2].encode('utf-8'), 
                                       metadata={"createdDateTime": row[3], 
                                                 "resultReportId":row[0]})

    insertCount += 1
    if not (insertCount%50):
        print(f"\tInserted count - {insertCount}")
    
    # read the inserted data from MongoDB -- only for verification
    # for grid_data in fs.find({"metadata.resultReportId": row[0]}, no_cursor_timeout=True):
    #     data = grid_data.read()
    #     print(data)

    # break
    # fetch next row from mssql and repeat
    row = selectRRCursor.fetchone()  

end = time.time()
selectRRCursor.close()

total_time = end - start
print("\nFinished - ResultReport Table")
print(f"\tTime Taken : {str(total_time)} \n")

mssqlConn.close()
mdbClient.close()

    






