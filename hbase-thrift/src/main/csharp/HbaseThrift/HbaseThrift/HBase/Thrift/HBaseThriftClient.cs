using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HbaseThrift.HBase.Thrift
{
    class HBaseThriftClient : AbstractHBaseThriftService
    {
        public HBaseThriftClient() : this("localhost", 9090)
        {

        }

        public HBaseThriftClient(string host, int port) : base(host, port)
        {
            
        }

        public override List<string> GetTables()
        {
            List<byte[]> tables = client.getTableNames();
            List<String> list = new List<String>();
            foreach(byte[] table in tables)
            {
                list.Add(Decode(table));
            }
            return list;
        }

        public override void Update(string table, string rowKey, bool writeToWal, string fieldName, string fieldValue, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] row = Encode(rowKey);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            List<Mutation> mutations = new List<Mutation>();
            Mutation mutation = new Mutation();
            mutation.IsDelete = false;
            mutation.WriteToWAL = writeToWal;
            mutation.Column = Encode(fieldName);
            mutation.Value = Encode(fieldValue);
            mutations.Add(mutation);
            client.mutateRow(tableName, row, mutations, encodedAttributes);
        }

        public override void Update(string table, string rowKey, bool writeToWal, Dictionary<string, string> fieldNameValues, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] row = Encode(rowKey);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            List<Mutation> mutations = new List<Mutation>();
            foreach (KeyValuePair<String, String> pair in fieldNameValues)
            {
                Mutation mutation = new Mutation();
                mutation.IsDelete = false;
                mutation.WriteToWAL = writeToWal;
                mutation.Column = Encode(pair.Key);
                mutation.Value = Encode(pair.Value);
                mutations.Add(mutation);
            }
            client.mutateRow(tableName, row, mutations, encodedAttributes);
        }

        public override void DeleteCell(string table, string rowKey, bool writeToWal, string column, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] row = Encode(rowKey);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            List<Mutation> mutations = new List<Mutation>();
            Mutation mutation = new Mutation();
            mutation.IsDelete = true;
            mutation.WriteToWAL = writeToWal;
            mutation.Column = Encode(column);
            mutations.Add(mutation);
            client.mutateRow(tableName, row, mutations, encodedAttributes);
        }

        public override void DeleteCells(string table, string rowKey, bool writeToWal, List<string> columns, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] row = Encode(rowKey);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            List<Mutation> mutations = new List<Mutation>();
            foreach (string column in columns)
            {
                Mutation mutation = new Mutation();
                mutation.IsDelete = true;
                mutation.WriteToWAL = writeToWal;
                mutation.Column = Encode(column);
                mutations.Add(mutation);
            }
            client.mutateRow(tableName, row, mutations, encodedAttributes);
        }

        public override void DeleteRow(string table, string rowKey, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] row = Encode(rowKey);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            client.deleteAllRow(tableName, row, encodedAttributes);
        }

        public override int ScannerOpen(string table, string startRow, List<string> columns, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] start = Encode(startRow);
            List<byte[]> encodedColumns = EncodeStringList(columns);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            return client.scannerOpen(tableName, start, encodedColumns, encodedAttributes);
        }

        public override int ScannerOpen(string table, string startRow, string stopRow, List<string> columns, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] start = Encode(startRow);
            byte[] stop = Encode(stopRow);
            List<byte[]> encodedColumns = EncodeStringList(columns);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            return client.scannerOpenWithStop(tableName, start, stop, encodedColumns, encodedAttributes);
        }

        public override int ScannerOpenWithPrefix(string table, string startAndPrefix, List<string> columns, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] prefix = Encode(startAndPrefix);
            List<byte[]> encodedColumns = EncodeStringList(columns);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            return client.scannerOpenWithPrefix(tableName, prefix, encodedColumns, encodedAttributes);
        }

        public override int ScannerOpenTs(string table, string startRow, List<string> columns, long timestamp, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] start = Encode(startRow);
            List<byte[]> encodedColumns = EncodeStringList(columns);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            return client.scannerOpenTs(tableName, start, encodedColumns, timestamp, encodedAttributes);
        }

        public override int ScannerOpenTs(string table, string startRow, string stopRow, List<string> columns, long timestamp, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] start = Encode(startRow);
            byte[] stop = Encode(stopRow);
            List<byte[]> encodedColumns = EncodeStringList(columns);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            return client.scannerOpenWithStopTs(tableName, start, stop, encodedColumns, timestamp, encodedAttributes);
        }

        public override List<TRowResult> ScannerGetList(int id, int nbRows)
        {
            return client.scannerGetList(id, nbRows);
        }

        public override List<TRowResult> ScannerGet(int id)
        {
            return client.scannerGet(id);
        }

        public override List<TRowResult> GetRow(string table, string row, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            byte[] startRow = Encode(row);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            return client.getRow(tableName, startRow, encodedAttributes);
        }

        public override List<TRowResult> GetRows(string table, List<string> rows, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            List<byte[]> encodedRows = EncodeStringList(rows);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            return client.getRows(tableName, encodedRows, encodedAttributes);
        }

        public override List<TRowResult> GetRowsWithColumns(string table, List<string> rows, List<string> columns, Dictionary<string, string> attributes)
        {
            byte[] tableName = Encode(table);
            List<byte[]> encodedRows = EncodeStringList(rows);
            List<byte[]> encodedColumns = EncodeStringList(columns);
            Dictionary<byte[], byte[]> encodedAttributes = EncodeAttributes(attributes);
            return client.getRowsWithColumns(tableName, encodedRows, encodedColumns, encodedAttributes);
        }

        public override void ScannerClose(int id)
        {
            client.scannerClose(id);
        }

        public override void IterateResults(TRowResult result)
        {
            foreach (KeyValuePair<byte[], TCell> pair in result.Columns)
            {
                Console.WriteLine("\tCol=" + Decode(pair.Key) + ", Value=" + Decode(pair.Value.Value));
            }
        }

        private String Decode(byte[] bs)
        {
            return UTF8Encoding.Default.GetString(bs);
        }

        private byte[] Encode(String str)
        {
            return UTF8Encoding.Default.GetBytes(str);
        }

        private Dictionary<byte[], byte[]> EncodeAttributes(Dictionary<String, String> attributes)
        {
            Dictionary<byte[], byte[]> encodedAttributes = new Dictionary<byte[], byte[]>();
            foreach (KeyValuePair<String, String> pair in attributes)
            {
                encodedAttributes.Add(Encode(pair.Key), Encode(pair.Value));
            }
            return encodedAttributes;
        }

        private List<byte[]> EncodeStringList(List<String> strings) 
        {
            List<byte[]> list = new List<byte[]>();
            if (strings != null)
            {
                foreach (String str in strings)
                {
                    list.Add(Encode(str));
                }
            }
            return list;
        }
    }
}
