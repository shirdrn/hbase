using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Thrift.Transport;
using Thrift.Protocol;

namespace HbaseThrift.HBase.Thrift
{
    public abstract class AbstractHBaseThriftService
    {
        protected static readonly string CHARSET = "UTF-8";
	    private string host = "localhost";
	    private int port = 9090;
	    private readonly TTransport transport;
	    protected readonly Hbase.Client client;

        public AbstractHBaseThriftService() : this("localhost", 9090)
        {
            
        }

        public AbstractHBaseThriftService(string host, int port)
        {
            this.host = host;
            this.port = port;
            transport = new TSocket(host, port);
            TProtocol protocol = new TBinaryProtocol(transport, true, true);
            client = new Hbase.Client(protocol);
        }

        public void Open() {
            if (transport != null)
            {
                transport.Open();
            }
	    }

        public void Close()
        {
            if (transport != null)
            {
                transport.Close();
            }
        }

        public abstract List<string> GetTables();
	
	    public abstract void Update(string table, string rowKey, bool writeToWal,
			string fieldName, string fieldValue, Dictionary<string, string> attributes);
        public abstract void Update(string table, string rowKey, bool writeToWal,
			Dictionary<string, string> fieldNameValues, Dictionary<string, string> attributes);
	
	    public abstract void DeleteCell(string table, string rowKey, bool writeToWal,
			    string column, Dictionary<string, string> attributes);
	    public abstract void DeleteCells(string table, string rowKey, bool writeToWal,
			    List<string> columns, Dictionary<string, string> attributes);
	
	     public abstract void DeleteRow(string table, string rowKey,
		            Dictionary<string, string> attributes);
		        
	    public abstract int ScannerOpen(string table, string startRow, List<string> columns,
	            Dictionary<string, string> attributes);
	    public abstract int ScannerOpen(string table, string startRow, string stopRow, List<string> columns,
	            Dictionary<string, string> attributes);
	    public abstract int ScannerOpenWithPrefix(string table, string startAndPrefix,
                List<string> columns, Dictionary<string, string> attributes);
	    public abstract int ScannerOpenTs(string table, string startRow,
	            List<string> columns, long timestamp, Dictionary<string, string> attributes);
	    public abstract int ScannerOpenTs(string table, string startRow, string stopRow,
	            List<string> columns, long timestamp, Dictionary<string, string> attributes);
		        
	    public abstract List<TRowResult> ScannerGetList(int id, int nbRows);
	    public abstract List<TRowResult> ScannerGet(int id);
	
	    public abstract List<TRowResult> GetRow(string table, string row,
		            Dictionary<string, string> attributes);
	    public abstract List<TRowResult> GetRows(string table,
                 List<string> rows, Dictionary<string, string> attributes);
	    public abstract List<TRowResult> GetRowsWithColumns(string table,
                 List<string> rows, List<string> columns, Dictionary<string, string> attributes);
	
	    public abstract void ScannerClose(int id);
	
	    /**
	     * Iterate result rows(just for test purpose)
	     * @param result
	     */
	    public abstract void IterateResults(TRowResult result);

    }
}
