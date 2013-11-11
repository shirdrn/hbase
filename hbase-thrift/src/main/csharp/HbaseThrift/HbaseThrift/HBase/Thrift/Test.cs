using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HbaseThrift.HBase.Thrift
{
    class Test
    {
        private readonly AbstractHBaseThriftService client;

        public Test(String host, int port)
        {
            client = new HBaseThriftClient(host, port);
        }

        public Test() : this("master", 9090)
        {
            
        }

        static String RandomlyBirthday()
        {
            Random r = new Random();
            int year = 1900 + r.Next(100);
            int month = 1 + r.Next(12);
            int date = 1 + r.Next(30);
            return year + "-" + month.ToString().PadLeft(2, '0') + "-" + date.ToString().PadLeft(2, '0');
        }

        static String RandomlyGender()
        {
            Random r = new Random();
            int flag = r.Next(2);
            return flag == 0 ? "M" : "F";
        }

        static String RandomlyUserType()
        {
            Random r = new Random();
            int flag = 1 + r.Next(10);
            return flag.ToString();
        }

        public void Close()
        {
            client.Close();
        }

        public void CaseForUpdate() {
		    bool writeToWal = false;
            Dictionary<String, String> attributes = new Dictionary<String, String>(0);
		    string table = SetTable();
		    // put kv pairs
		    for (int i = 0; i < 10000000; i++) {
                string rowKey = i.ToString().PadLeft(4, '0');
                Dictionary<String, String> fieldNameValues = new Dictionary<String, String>();
			    fieldNameValues.Add("info:birthday", RandomlyBirthday());
			    fieldNameValues.Add("info:user_type", RandomlyUserType());
			    fieldNameValues.Add("info:gender", RandomlyUserType());
			    client.Update(table, rowKey, writeToWal, fieldNameValues, attributes);
		    }
	    }

        public void CaseForDeleteCells() {
		    bool writeToWal = false;
            Dictionary<String, String> attributes = new Dictionary<String, String>(0);
		    String table = SetTable();
		    // put kv pairs
		    for (long i = 5; i < 10; i++) {
			    String rowKey = i.ToString().PadLeft(4, '0');
			    List<String> columns = new List<String>(0);
			    columns.Add("info:birthday");
			    client.DeleteCells(table, rowKey, writeToWal, columns, attributes);
		    }
	    }

        public void CaseForDeleteRow() {
		    Dictionary<String, String> attributes = new Dictionary<String, String>(0);
		    String table = SetTable();
		    // delete rows
		    for (long i = 5; i < 10; i++) {
			    String rowKey = i.ToString().PadLeft(4, '0');
			    client.DeleteRow(table, rowKey, attributes);
		    }
	    }
	
	    public void CaseForScan() {
		    Dictionary<String, String> attributes = new Dictionary<String, String>(0);
		    String table = SetTable();
		    String startRow = "0005";
		    String stopRow = "0015";
		    List<String> columns = new List<String>(0);
		    columns.Add("info:birthday");
		    int id = client.ScannerOpen(table, startRow, stopRow, columns, attributes);
		    int nbRows = 2;
		    List<TRowResult> results = client.ScannerGetList(id, nbRows);
		    while(results != null) {
			    foreach(TRowResult result in results) {
				    client.IterateResults(result);
			    }
			    results = client.ScannerGetList(id, nbRows);
		    }
		    client.ScannerClose(id);
	    }
	
	    public void CaseForGet() {
		    Dictionary<String, String> attributes = new Dictionary<String, String>(0);
		    String table = SetTable();
		    List<String> rows = new List<String>(0);
		    rows.Add("0009");
		    rows.Add("0098");
		    rows.Add("0999");
		    List<String> columns = new List<String>(0);
		    columns.Add("info:birthday");
		    columns.Add("info:gender");
		    List<TRowResult> results = client.GetRowsWithColumns(table, rows, columns, attributes);
		    foreach(TRowResult result in results) {
			    client.IterateResults(result);
		    }
	    }

        private string SetTable()
        {
            string table = "test_info";
            return table;
        }

        static void Main(string[] args)
        {
            Test test = new Test();
            //test.CaseForUpdate(); // insert or update rows/cells
            //test.CaseForDeleteCells(); // delete cells
            //test.CaseForDeleteRow(); // delete rows
            test.CaseForScan(); // scan rows
            //test.CaseForGet(); // get rows

            test.Close();
        }

    }
}
