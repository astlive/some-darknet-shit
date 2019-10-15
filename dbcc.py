import pymysql 

class Dbcc:
    def __init__(self, host, port, database, user, password):
        self._db = pymysql.connect(autocommit=True, host=host, port=port, user=user, password=password, database=database, charset='utf8')
        
    def chk_db(self):
        with self._db.cursor() as cur:
            cur.execute("select 1")
            rr = cur.fetchall()
            if(len(rr)>0):
                return True
            else:
                return False

    def get_job(self):
        with self._db.cursor() as cur:
            sql = 'SELECT * FROM file WHERE file.status = 0'
            cur.execute(sql)
            rr = cur.fetchall()
            return rr
            
    def query(self, sqlcmd):
        with self._db.cursor() as cur:
            cur.execute(sqlcmd)
            rr = cur.fetchall()
            return rr

    def updatefilestatus(self, status, fid):
        with self._db.cursor() as cur:
            cur.execute("UPDATE file SET file.status = %s WHERE file.id = %s", (str(status), str(fid)))
            rr = cur.fetchall()
            return rr
    
def main():
    print("MySQL connect Test")
    db = Dbcc()
    print(db.chk_db())
    print("sql test")
    print("1. add a test mp4 to sql")
    print("2. update the test mp4 record to unchanged")
    print("3. remove the test record")
    todo = input()
    if(todo is "1"):
        pass
    elif todo is "2":
        pass
    elif todo is "3":
        pass
    
if __name__ == '__main__':
    main()