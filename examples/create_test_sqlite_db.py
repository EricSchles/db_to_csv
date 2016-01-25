import sqlite3
    
con = sqlite3.connect("foo.db")
cur = con.cursor()
cur.execute("CREATE TABLE hello (hi text)")
cur.execute("insert into hello values ('hiiii')")
con.commit()
con.close()
