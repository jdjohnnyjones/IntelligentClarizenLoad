using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.IO.Compression;
using System.Data.SqlClient;

namespace IntelligentClarizenLoad
{
    class Program
    {
        static string sqlConnString =
                //"Server=tcp:samtec-dw-dev-sqlserver.database.windows.net,1433;Initial Catalog=SamtecDW-Dev;Persist Security Info=False;User ID=clarizen;Password=knOLH(*&346;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=300;";
                "Server = tcp:clarizen-dev-dbserver.database.windows.net,1433;Initial Catalog = clarizen_dev; Persist Security Info=False;User ID = clarizen; Password=knOLH(*&346; MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout = 300;";

        static void Main(string[] args)
        {
            var storageConnString = "DefaultEndpointsProtocol=https;AccountName=clarizendevdatastore;AccountKey=OpSyRg1YhVIoz0ji4lD588ic77cY1kZB+jqwJsyD4rJ63dtsGhuG9tn6MnLVZ60M804FFTJAF23U72TbIWfxmg==";
            int readyToProcess;
            string containerName = "clarizendw";
            string blobPrefix = "daily";

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // get list of all blobs
            Dictionary<string, string> blobDirectoryList = new Dictionary<string, string>();

            // derive dirs from blobs
            foreach (var item in container.ListBlobs(blobPrefix, useFlatBlobListing: true))
            {
                CloudBlockBlob blob = (CloudBlockBlob)item;
                string blobDirectory = ParseBlobUri(blob.Uri, "virtualDirectory");
                string blobDate = ParseBlobUri(blob.Uri, "date").Replace("_", "-");
                if (!blobDirectoryList.Keys.Contains(blobDirectory))
                    blobDirectoryList.Add(blobDirectory, blobDate);
            }

            // loop through and process
            foreach (var item in blobDirectoryList.OrderBy(pair => pair.Value)) // order by blob date, derived from blob name
            {
                // gather blob information for processing
                string blobDirectory = item.Key;
                string blobDate = item.Value;
                CloudBlockBlob blobReference = GetTopBlobFromDirectory(container, blobDirectory);
                string tableName = ParseBlobUri(blobReference.Uri, "table").Trim('/');

                readyToProcess = GetProcessReadyStatus(container, blobDirectory, tableName, blobDate, sqlConnString);

                if (readyToProcess == 1) // process blob if hasn't yet been processed and table not in failed state
                {
                    ProcessBlobs(container, blobDirectory, blobDate, blobReference, tableName, sqlConnString);
                }
            }

            //return new Dictionary<string, string>(); // return empty to statisfy class; could be used to chain activities in future
        }

        public static int ProcessBlobs(CloudBlobContainer blobContainer, string blobDirectory, string blobDate
            , CloudBlockBlob blobReference, string tableName, string sqlConnString)
        {
            int loadStatus = 0;
            string formattedColumns;
            string loadId = Guid.NewGuid().ToString();

            try
            {
                // insert record that load is starting
                UpdateLoadLog(blobDirectory, tableName, loadId, sqlConnString);

                // get number of blobs in virtual dir
                string blobCountAsString = GetBlobCountInVirtualDirectory(blobContainer, blobDirectory);

                // process text in blobs and return columns
                formattedColumns = ProcessText(blobContainer, blobDirectory, tableName, sqlConnString);

                // start process to load table
                LoadTable(blobContainer, blobDirectory, tableName, blobCountAsString, loadId, formattedColumns, sqlConnString);

                // update log record indicating success
                UpdateLoadLog(loadId, 0, tableName, blobDate, sqlConnString);
            }
            catch (Exception ex)
            {
                loadStatus = 1;
                UpdateLoadLog(loadId, 1, tableName, blobDate, sqlConnString, ex.Message);
            }

            return loadStatus;
        }

        public static string ProcessText(CloudBlobContainer blobContainer, string blobDirectory, string tableName, string sqlConnString)
        {
            // decompress, replace any back-to-back double quotes,
            // grab columns for processing, delete decompressed blobs
            // or compress and overwrite if text was manipulated
            string oldContent = "";
            string newContent = "";
            string processStep = "Process Text";
            string formattedColumns = "";
            CloudBlobDirectory dir = blobContainer.GetDirectoryReference(blobDirectory);

            try
            {
                foreach (var item in dir.ListBlobs(useFlatBlobListing: true))
                {
                    CloudBlockBlob compBlob = (CloudBlockBlob)item;
                    CloudBlockBlob decompBlob = DecompressBlob(blobContainer, compBlob);

                    // read contents of blob
                    using (StreamReader readStream = new StreamReader(decompBlob.OpenRead()))
                    {
                        if (formattedColumns == "")
                            formattedColumns = ProcessColumns(readStream.ReadLine(), tableName, sqlConnString);
                    }

                    using (StreamReader readStream = new StreamReader(decompBlob.OpenRead()))
                        oldContent = readStream.ReadToEnd();

                    // if blob contains "" in any lines, replace with "
                    // polybase doesn't support escaping quotes
                    if (oldContent.Contains("\"\""))
                    {
                        newContent = oldContent.Replace("\"\"", "");

                        // upload altered blob
                        using (StreamWriter writeStream = new StreamWriter(decompBlob.OpenWrite()))
                            writeStream.Write(newContent);

                        int compressStatus = CompressBlob(blobContainer, decompBlob);

                        if (compressStatus == 0)
                            decompBlob.Delete();
                    }
                    else
                        decompBlob.Delete();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error at step: " + processStep + " :: " + ex.Message);
            }

            return formattedColumns;
        }

        public static CloudBlockBlob DecompressBlob(CloudBlobContainer blobContainer, CloudBlockBlob compBlob)
        {
            string processStep = "Decompress Blob";
            string compBlobName = ParseBlobUri(compBlob.Uri, "name");
            string decompBlobName = compBlobName.Substring(0, compBlobName.Length - 3);
            CloudBlockBlob decompBlob = blobContainer.GetBlockBlobReference(
                        ParseBlobUri(compBlob.Uri, "virtualDirectory") + decompBlobName);
            byte[] data;

            try
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    compBlob.DownloadToStream(ms);
                    ms.Seek(0, SeekOrigin.Begin);
                    data = ms.ToArray();
                }

                using (MemoryStream comp = new MemoryStream(data))
                {
                    using (MemoryStream decomp = new MemoryStream())
                    {
                        using (GZipStream gzip = new GZipStream(comp, CompressionMode.Decompress))
                        {
                            gzip.CopyTo(decomp);
                        }

                        decomp.Position = 0;
                        decompBlob.UploadFromStream(decomp);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error at step: " + processStep + " :: " + ex.Message);
            }

            return decompBlob;
        }

        public static int CompressBlob(CloudBlobContainer blobContainer, CloudBlockBlob decompBlob)
        {
            int compStatus = 0;
            string processStep = "Compress Blob";
            string decompBlobName = ParseBlobUri(decompBlob.Uri, "name");
            string compBlobName = decompBlobName + ".gz";
            CloudBlockBlob compBlob = blobContainer.GetBlockBlobReference(
                ParseBlobUri(decompBlob.Uri, "virtualDirectory") + compBlobName);

            byte[] data;

            try
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    decompBlob.DownloadToStream(ms);
                    ms.Seek(0, SeekOrigin.Begin);
                    data = ms.ToArray();
                }

                using (MemoryStream comp = new MemoryStream())
                {
                    using (GZipStream gzip = new GZipStream(comp, System.IO.Compression.CompressionLevel.Fastest))
                    {
                        gzip.Write(data, 0, data.Length);
                    }

                    data = comp.ToArray();
                }

                compBlob.UploadFromByteArray(data, 0, data.Length);

                compStatus = 0;
            }
            catch (Exception ex)
            {
                compStatus = 1;
                throw new Exception("Error at step: " + processStep + " :: " + ex.Message);
            }

            return compStatus;
        }

        public static int GetProcessReadyStatus(CloudBlobContainer blobContainer, string blobDirectory
            , string tableName, string blobDate, string sqlConnString)
        {
            int readyToProcess = 0;
            blobDate = blobDate.Replace("_", "-");

            using (SqlConnection sqlCon = new SqlConnection(sqlConnString))
            {
                sqlCon.Open();
                SqlCommand command = new SqlCommand("uspGetProcessReadyStatus", sqlCon);
                command.CommandType = System.Data.CommandType.StoredProcedure;
                command.Parameters.Add(new SqlParameter("@tableName", tableName));
                command.Parameters.Add(new SqlParameter("@blobDate", blobDate));
                command.Parameters.Add(new SqlParameter("@readyToProcess", readyToProcess)).Direction = System.Data.ParameterDirection.Output;
                command.ExecuteNonQuery();
                readyToProcess = Convert.ToInt32(command.Parameters["@readyToProcess"].Value.ToString());
            }

            return readyToProcess;
        }

        public static int LoadTable(CloudBlobContainer blobContainer, string blobDirectory, string tableName,
            string blobCountAsString, string loadId, string columns, string sqlConnString)
        {
            int status = 0;
            string processStep = "Load Table";

            try
            {
                using (SqlConnection sqlCon = new SqlConnection(sqlConnString))
                {
                    sqlCon.Open();
                    SqlCommand command = new SqlCommand("uspLoadTable", sqlCon);
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@tableName", tableName));
                    command.Parameters.Add(new SqlParameter("@columns", columns));
                    command.Parameters.Add(new SqlParameter("@blobDirectory", blobDirectory));
                    command.Parameters.Add(new SqlParameter("@rejectCount", blobCountAsString));
                    command.CommandTimeout = 1200;
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                status = 1;
                throw new Exception("Error at step: " + processStep + " :: " + ex.Message);
            }

            return status;
        }

        public static string GetBlobCountInVirtualDirectory(CloudBlobContainer blobContainer, string blobDirectory)
        {
            string processStep = "Get Blob Count";
            string blobCountAsString;

            try
            {
                CloudBlobDirectory dir = blobContainer.GetDirectoryReference(blobDirectory);
                blobCountAsString = dir.ListBlobs().Count().ToString();
            }
            catch (Exception ex)
            {
                throw new Exception("Error at step: " + processStep + " :: " + ex.Message);
            }

            return blobCountAsString;

        }

        public static CloudBlockBlob GetTopBlobFromDirectory(CloudBlobContainer blobContainer, string blobDirectory)
        {
            CloudBlobDirectory dir = blobContainer.GetDirectoryReference(blobDirectory);
            List<CloudBlockBlob> blobs = new List<CloudBlockBlob>();

            foreach (var item in dir.ListBlobs())
            {
                blobs.Add((CloudBlockBlob)item);
            }

            return blobs.First();
        }

        public static string ProcessColumns(string columns, string tableName, string sqlConnString)
        {
            IList<KeyValuePair<string, string>> dataList = GetColumnsAndDataTypes(tableName, sqlConnString);

            string formattedColumns = "";
            string[] columnArray = columns.Split('|');
            string dataType;
            string alteredColumn;

            foreach (string column in columnArray)
            {
                dataType = dataList.Where(k => k.Key == column).Select(v => v.Value).ToList().DefaultIfEmpty("varchar(200)").First();
                alteredColumn = ", [" + column + "] " + dataType;
                columnArray.SetValue(alteredColumn, Array.IndexOf(columnArray, column));
                formattedColumns += alteredColumn;
            }

            // remove the first comma
            formattedColumns = formattedColumns.Remove(0, 2);

            return formattedColumns;
        }

        public static string ParseBlobUri(Uri blobUri, string requestedSegment)
        {
            string uriSegment;

            if (requestedSegment == "daily")
                uriSegment = blobUri.Segments.ElementAt(2);
            else if (requestedSegment == "date")
                uriSegment = blobUri.Segments.ElementAt(3).Substring(0, 10);
            else if (requestedSegment == "table")
                uriSegment = blobUri.Segments.ElementAt(4);
            else if (requestedSegment == "name")
                uriSegment = blobUri.Segments.ElementAt(5);
            else if (requestedSegment == "virtualDirectory")
                uriSegment = blobUri.Segments.ElementAt(2) + blobUri.Segments.ElementAt(3) + blobUri.Segments.ElementAt(4);
            else
                uriSegment = "";

            return uriSegment;
        }

        public static int UpdateLoadLog(string blobDirectory, string tableName, string loadId, string sqlConnString)
        {
            int status = 0;
            string processStep = "Insert Load Start";

            // starting a new load
            try
            {
                using (SqlConnection sqlCon = new SqlConnection(sqlConnString))
                {
                    sqlCon.Open();
                    SqlCommand command = new SqlCommand("uspUpdateLoadLogStart", sqlCon);
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@loadId", loadId));
                    command.Parameters.Add(new SqlParameter("@tableName", tableName));
                    command.Parameters.Add(new SqlParameter("@blobDirectory", blobDirectory));
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                status = 1;
                throw new Exception("Error at step: " + processStep + " :: " + ex.Message);
            }

            return status;
        }

        public static void UpdateLoadLog(string loadId, int runResult, string tableName
            , string blobDate, string sqlConnString, string loadMessage = "")
        {
            // updating for completion
            using (SqlConnection sqlCon = new SqlConnection(sqlConnString))
            {
                sqlCon.Open();
                SqlCommand command = new SqlCommand("uspUpdateLoadLogComplete", sqlCon);
                command.CommandType = System.Data.CommandType.StoredProcedure;
                command.Parameters.Add(new SqlParameter("@loadId", loadId));
                command.Parameters.Add(new SqlParameter("@runResult", runResult));
                command.Parameters.Add(new SqlParameter("@loadMessage", loadMessage));
                command.Parameters.Add(new SqlParameter("@tableName", tableName));
                command.Parameters.Add(new SqlParameter("@blobDate", blobDate));
                command.ExecuteNonQuery();
            }

        }

        public static IList<KeyValuePair<string, string>> GetColumnsAndDataTypes(string tableName, string sqlConnString)
        {
            // pull down columns and data types from sql
            IList<KeyValuePair<string, string>> dataList = new List<KeyValuePair<string, string>>();

            using (SqlConnection sqlCon = new SqlConnection(sqlConnString))
            {
                sqlCon.Open();
                string commandString = @"select ColumnName, SqlDataType
                                        from dbo.DataDictionary
                                        where TableName = @tableName";
                SqlCommand command = new SqlCommand(commandString, sqlCon);
                command.Parameters.Add(new SqlParameter("@tableName", tableName));
                command.CommandType = System.Data.CommandType.Text;

                SqlDataReader sr = command.ExecuteReader();

                if (sr.HasRows)
                {
                    while (sr.Read())
                    {
                        dataList.Add(new KeyValuePair<string, string>(sr["ColumnName"].ToString(), sr["SqlDataType"].ToString()));
                    }
                }

            }

            return dataList;
        }
    }
}
