import sys, sqlite3

class sqlMerge(object):
    """Basic python script to merge data of 2 !!!IDENTICAL!!!! SQL tables"""

    def __init__(self, parent=None):
        super(sqlMerge, self).__init__()

        self.dbA = None
        self.dbB = None


    def loadTables(self, fileNameA, fileNameB):
        self.dbA = sqlite3.connect(fileNameA)
        self.dbB = sqlite3.connect(fileNameB)

        cursorA = self.dbA.cursor()
        cursorA.execute("SELECT name FROM sqlite_master WHERE type='table';")

        table_counter = 0
        print("SQL Tables available: \n===================================================")
        for tableItem in cursorA.fetchall():
            current_table = tableItem[0]
            table_counter += 1
            print("-> " + current_table)
        print("\n===================================================")

        if table_counter == 1:
            table_to_merge = current_table
        else:
            table_to_merge = input("Table to Merge: ")

        return table_to_merge

    def merge(self, tableName):
        cursorA = self.dbA.cursor()
        cursorB = self.dbB.cursor()

        newTableName = tableName + "_new"

        try:
            cursorA.execute("CREATE TABLE IF NOT EXISTS " + newTableName + " AS SELECT * FROM " + tableName)
            for row in cursorB.execute("SELECT * FROM " + tableName):
                print(row)
                cursorA.execute("INSERT INTO " + newTableName + " VALUES" + str(row) +";")

            cursorA.execute("DROP TABLE IF EXISTS " + tableName);
            cursorA.execute("ALTER TABLE " + newTableName + " RENAME TO " + tableName);
            self.dbA.commit()

            print("\n\nMerge Successful!\n")

        except sqlite3.OperationalError:
            print("ERROR!: Merge Failed")
            cursorA.execute("DROP TABLE IF EXISTS " + newTableName);

        finally:
            self.dbA.close()
            self.dbB.close()

        return

    def main(self):
        print("Please enter name of db file")
        fileNameA = input("File Name A:")
        fileNameB = input("File Name B:")

        tableName = self.loadTables(fileNameA, fileNameB)
        self.merge(tableName)

        return

if __name__ == '__main__':
    app = sqlMerge()
    app.main()
