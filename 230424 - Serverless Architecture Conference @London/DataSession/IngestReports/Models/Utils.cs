using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IngestReports.Models
{
    public static class Utils
    {
        private static Random _random = new Random();
        private static string[] _countries = new string[] { "IT", "US", "NL", "PL", "DE", "JP" };
        private static string[] _accounts = new string[] { "ad6c423d-afa5-4236-ad33-74a5c56dde2d", "47b3de25-af97-466c-baf6-5e26e8a1b4d9", "fb6628cb-cb87-4dee-a836-aa0193070b02", "db65103e-ce9b-4022-bbf4-f3de44b280a8", "fb4e93f8-fbfd-4d39-ab72-128aec873720", "dfbdc95d-f124-4669-a89b-2ebc10e10173", "3d6a8868-1150-4d9a-af31-47e9af1967bf", "a81b2308-c6f5-415b-8fe2-238893ca4f02", "77158ae5-7334-4654-b3fa-8cdfa37b2016", "c2b68623-07cb-4e6f-bbed-8eb75841bc9b", "99dc837d-681d-4ca9-bcd8-7d4e7ace18c1", "a595d693-fe85-4bdd-8e55-dcb276e1b555", "0b9243f0-fe3b-4efa-b50a-5b2bb10430d8", "bed95548-f31a-4303-8df8-fb2d78534718", "2b76a9f4-1846-46ca-98f7-65ad9e5602bd", "47dec286-e4d4-4e24-91b6-aa50bca27135", "98ae7c71-61c2-4197-849e-22da3cf9c771", "cdf44704-0779-495f-97d7-7b425ab4dc93", "c0400a10-e7c5-4885-86cb-7f61b48cdfb2", "cf3c3a0b-d954-4cb9-bf4b-78c0115acc17", "6301b518-7005-4bd5-8329-7822aebf13ad", "782129dd-81d6-49aa-8237-2d2bc8f23b6c", "9e4876db-dea1-4197-9eb7-a92a1d4bd1f9", "17bf549c-18c1-470b-852b-224742561314", "cbef3185-ffb6-4e94-a762-4854bdd62080", "5e10738a-7aeb-4018-912c-5b539c7a478c", "2e9ea177-821b-4749-964a-3b8a76cd9fa3", "94a8ec22-4fa6-405b-bef5-841cb8a1cfe8", "e3149bde-152a-4adc-afc4-cfae7ef750e8", "834be345-847c-49eb-b49c-49872ae6ab35", "45d474c4-3c47-4cdf-a43a-2447629246ec", "11849ea8-2a74-403f-ab60-5021b5ba5a4c", "ddaf4369-e564-446d-afea-98868f964028", "19ca60c9-0add-4476-8b16-a769e92af5ac", "cded3098-0b32-4e0c-be75-363f62a832c8", "a11eea94-a35e-4d53-b456-b4ebe9367109", "9d38efae-6763-4987-a9a0-e44ab959eaf2", "51c730d2-c40d-4e17-af8c-1b53e1552ecb", "c1774b44-234d-4ca8-8c9f-84f3317396d9", "1fd78ec4-02f8-4267-968c-26772cd488df", "eac7c444-5b7c-452a-84a7-0f29603b6d65", "0a7f9581-8add-4815-8be5-06dea335b1a1", "1562ba10-d798-44a8-b1b4-b209e2564196", "7d810929-c467-40c5-aa2c-2aa7a1389007", "1777d026-cd0b-4c91-9e8a-4d58565d6191", "8e0d4e5c-f957-4125-854b-8185d8404efa", "514c8817-5090-4549-9257-ac42a2a68c56", "6fb69325-5767-42df-8264-f6ccd293d868", "6ed8d3ff-9944-463c-853e-ce363626b9b1", "2f8ce186-49c0-4129-8e72-fd10f43a50ac", "85aaa9f9-372b-4d3e-a4c8-571259ec4167", "dd215830-c077-4731-a4b0-06bed0cb8eb5", "484f7012-a4d8-4bde-8809-32453b908fb7", "f4cb4220-bcde-4801-8d59-68dc308d85f8", "dadc7bbe-7ab3-4f9e-b6a4-83d909f99c62", "3f1c71f5-7843-481e-a7d3-1d1f3ead265e", "91e60574-8675-419b-8cb2-3df557c20305", "03a104fd-d219-4b1b-8542-17bde91a2057", "4f97d5e7-56a5-4587-bcde-60ba3cac735d", "4a9c79db-7c10-44ae-a5e7-d7039fa0765c", "d954e9f3-bf8a-4ba2-a662-0df9b72f3415", "8af24c86-1674-4611-a49e-7b7507ac367f", "79b8846d-fb3e-4cab-b47f-8794ebbe2983", "b16ddd8f-a286-4dcc-ae26-d18490ab5a10", "b43cf357-9f3c-4cee-9576-0fa57ae061f9", "33d61769-1b97-4bc9-bd4a-61524cf7ca6a", "8f716dea-189a-4cee-9a4c-b1c7fc0f8206", "99017e4d-4de0-49ea-b32f-06df05268808", "89101ff6-c4a3-41e5-9006-275bc29f2e82", "8302fd4a-dcb0-40ef-b69e-747de856f76c", "5b7bc043-5d18-4a37-8965-1488f73204d2", "2ea10cd9-f278-4657-922a-f2a3ee409198", "fe91fd9c-eb3f-4e62-95bf-93e5ad750b2d", "d9d336b9-39af-4aee-bd5d-e30e1a7de8b4", "94151b64-80d0-4b2b-94c5-2b91fac410ef", "21a7abe5-1dcb-451c-b936-e969c9b36b33", "0825ccfb-e241-4c35-85ee-941513152826", "ec0c0125-018f-4b1d-a219-c9da21c5d826", "c6ff727d-b750-48db-91b4-bc903716c9ff", "c0f1f9bf-5627-4ee3-ad1f-0ffc1dde796d", "38f5408e-8b6a-4e43-af45-9bf6a5911017", "726c9eec-304d-42c5-b3e0-80fd69a23f0b", "87748bc6-5adb-456b-83d1-1ae1366e978f", "55d37e34-81e0-434f-b1c2-4ecbf3b7e905", "0016b2ad-765c-495e-afd6-2c17df160a9c", "00699c0a-c492-4a0c-8278-21eccfdf8ec1", "dca80479-86ab-4da3-8836-611d50339cbf", "233a1cff-ced7-4ee6-bca7-4118e2befe8d", "a97252e7-13a4-454a-bbbe-a1cf929e5ed6", "8834fa6f-467c-4961-90bb-34b174c06f3b", "7721673f-7ace-455a-8d6a-e9cd74affa29", "564d930a-2744-4d0a-a30f-71c6b45bb2b9", "f224df8f-f572-416a-ab8c-e51daa73c429", "9611876f-fc7e-460b-9511-d4a4883ac140", "a68f9308-2db3-48ae-802b-408af13fc192", "c4f761f3-d3d3-4509-9950-2fb35056a2c3", "6fe421d4-e6f6-4b31-8a79-d14308b9d99d", "12dd0e24-b0f8-42ee-be6c-86a0e23e2da4", "48237ded-c8f3-4918-b670-02dfe9873ff8", "49850641-8efb-447b-90d2-541a73ceb49e" };
        private static string[] _campaigns = new string[] { "9e938530-70d7-48cf-b552-124b1b066b8f", "25a14ac1-164a-4aed-a34c-54a37b3cc65a", "55e14329-66ea-4668-9910-a87f44364004", "0c587594-cd2e-41b4-be36-33c648164492", "a4e928df-be66-4bd3-a9e3-f8275d512bc9", "7743768f-fdd2-4da1-a3cf-1765f12b889f", "d75153d3-91c4-4a5d-9e07-4cc3d8e92e14", "f77da7e0-4c91-4785-9f20-e4b88a9e4685", "f2fcd105-63a8-4437-bb69-2dbdf9cfcb06", "e12397c7-e4b0-4f7f-a12e-0d50d2e5ae94", "7017eb96-c8ce-4def-b3c8-825fd7a43d73", "91aedb90-b386-4bfc-8e98-5531f47862f5", "b146adef-8817-4eb0-9543-27916c6511f0", "3e7624e2-b8c9-4f58-ba99-53a2c68b5d8e", "58d2bc00-bec6-4f66-ae64-f045e0d532b6", "94d94e69-8818-41d0-bcb6-38486f12599e", "9fb345da-8b64-4e7b-b7f7-8afc388f1d0f", "f7afb96a-5b01-4ad8-bbf4-59ad9df9738d", "45971d85-0f9b-40f5-ac7a-21df95908153", "8f16abc6-de67-4eb3-a674-6721e77f9eab", "3f70cd86-f0cf-4467-956b-68172d2992dc", "9b5564c0-3372-47e2-b687-1696cbeef20f", "49ee5ad5-5333-42fe-a069-95e072da5625", "443a0ded-735f-40ce-a022-b873746ee527", "d32da509-644f-445c-bc33-9575290ce42f", "52c53cba-abed-4558-b136-00e2ed3b674a", "e1e927e2-bca0-4571-b639-b11ae432a831", "606e7d3a-6b84-4714-b557-1cc033c73c4e", "45ceb6df-eb21-4e11-ac7d-c4676048da92", "5ee1d93a-21a0-45e1-8f26-c08b32d00959", "9846492d-61fe-4eb4-b9d5-f4ec218da5b2", "a3fe935c-86a8-447e-a189-41aa854d7680", "eded58eb-e800-4e2c-8442-88285da967e9", "f7c673f6-4a7b-4367-a2f4-2b6420858eaf", "cb1e79d9-6a78-4253-803d-66b565e8fda0", "aea81609-ffc8-4260-b01e-b50fc8eda95e", "59f34bd3-8757-486f-aec0-93d512395689", "f3cb5780-5b79-4c9a-9116-a2788836e016", "ccfa2bc3-8ba9-4250-ac20-451feed990f5", "f80653c8-78fe-4a15-bc59-880fb220f6f1", "1bf81641-a3b0-4e4a-b15b-1b721a74b0a8", "3acccd9a-f467-45fb-a179-34297ff7a328", "032b2028-2c0d-4ca2-b57b-1b33ad65fb16", "b0dc149c-bc4d-401d-ab11-e0b03e799b0f", "1b71c43b-8004-4cbe-9a10-817a6f5d1813", "a2f9aa46-f876-4a4f-b1f6-7daa0dc257db", "358b81f3-a507-4112-b6b9-612a22aba9f9", "db02c267-4870-412a-8ca2-f05d8addaef5", "3d883dac-0fcc-4524-9661-9231b63b1982", "017a1af5-8852-46b3-b32e-2939811be7ed", "1a71446b-f0cd-4f4c-bb7d-6f8c8d2cc991", "b910fbad-328e-4f31-83ad-ab184d7a1b17", "41365c60-7cf8-419c-a39f-f58494b3d8f3", "8a8aa1a9-b51e-4234-8ed3-d00c4df84dd3", "f97ea59d-45c3-4fba-995b-df03a9f5f290", "39516235-2977-48f5-9a75-b8692186ba70", "6784c0c9-a868-469b-ab1e-caaea95c9d19", "aa776bff-5728-4c59-be15-78f9abd4ec0e", "d3426040-5959-4977-8148-b54dda5140cc", "22883641-4727-41f4-8477-e47d0ddbe982", "af54cebf-7c2e-4177-addd-948af89e66e8", "bc3f076b-6b22-4fc3-b8bd-9bf3411828c7", "3ee23887-0ad0-4a2b-8919-975e08ef4e90", "56fa6106-70c3-455e-be35-c1e5f8a54cec", "460a3939-1810-4e7c-92b3-15a7394d02f0", "d49085e6-7c12-4f12-9958-a99cea34d4a5", "3ac74386-710b-4b1e-b787-c91aa60e96ec", "a26d2a6b-65ef-4d10-883b-5815d4add1d1", "c0e04d20-7983-4d86-970d-c752fd086202", "c9813cdc-1697-46b2-9d80-9bf8cb8e549a", "61f96da3-8e2e-4ffd-a2f9-cec7234023ab", "2b7d5c4f-0077-4679-92bb-b17d1e9a52d1", "5d1bb581-3344-45b2-a13c-836d85f948f6", "3d1855bd-e7d2-4dd2-8a7b-48597a41dc9a", "c44301e0-22a8-467c-adfd-8e86e3db8a4a", "ac82264a-db51-47a1-982c-3da1fb9a765a", "8ca89d8c-e391-43d4-9655-58a1becfb3f7", "4f32dac0-e8c5-44c3-a4ab-5ecdcaa9866a", "f1e8ab88-9b19-4eab-a17d-fcb71bcb97a2", "e563bd66-3df8-4bb8-b998-c75bdceeb7fd", "2a330ac0-4771-46a9-8847-af32e2e2b5ab", "417b734a-16d2-45ef-9db7-69dfc5d53e72", "26b37029-31f2-4af3-bdca-48605966a071", "7457a409-5adb-4cb4-b202-c2574deaeaf9", "ee8f8be5-9aa3-4fd5-91e3-37911d94b39d", "d01fdedf-684d-4ec5-859d-7e28ae87d2e0", "5adbabea-d156-4875-978b-bda6ea4d00ba", "541c7bc1-b03c-49c3-ada8-25e64842ca79", "cd4116a9-f50f-4161-ad89-160db17eeaeb", "9726b4ea-004a-4758-8f6b-f90ac7e955de", "d7750de6-91af-4937-ba4e-d14b9995a958", "cd195f2e-2f00-466a-ab7e-fd4c03e884b1", "7b56cade-809a-42d5-b8d7-bf2290f19c71", "6f9a0e39-7679-47b4-8964-afa50c6b75b0", "00811bf0-4614-41f6-b600-579b7f0f85cb", "c9e45753-7480-4b37-bd1b-f416ff84df8f", "22bb30dc-92f9-415f-9ce9-b61708a4579a", "72a076be-d934-4c0f-a0ad-6487daf931b4", "0d064d7a-42ce-4771-b735-0460bd408828", "ebe1f821-aec1-488b-a103-426a12a62b91" };
        private static string[] _creativities = new string[] { "a", "b", "c", "d", "e", "f" };
        public static string RandomCountry() => _countries[_random.Next(0, _countries.Length)];
        public static string RandomAccount() => _accounts[_random.Next(0, _accounts.Length)];
        public static string RandomCampaign() => _campaigns[_random.Next(0, _campaigns.Length)];
        public static string RandomCreativity() => _creativities[_random.Next(0, _creativities.Length)];

        public static DateTimeOffset RandomDateTime(DateTime? day = null)
        {
            return new DateTimeOffset(
                day?.Year ?? _random.Next(2020, 2022),
                day?.Month ?? _random.Next(1, 12),
                day?.Day ?? _random.Next(1, 28),
                _random.Next(0, 23),
                _random.Next(0, 59),
                _random.Next(0, 59), TimeSpan.FromSeconds(0));
        }

        public static double RandomSellout() => (_random.NextDouble() * 10000) + 1000;
        public static double RandomCost() => _random.NextDouble() * 1000;
        public static int RandomConversions() => _random.Next(0, 100);
        public static int RandomClicks() => _random.Next(1000, 50000);

        public static int Random(int min, int max) => _random.Next(min, max);


        public static DataTable AsTable<T>()
        {
            var dt = new DataTable();
            var props = typeof(T).GetProperties();
            foreach (var prop in props)
            {
                dt.Columns.Add(new DataColumn(prop.Name, prop.PropertyType));
            }
            return dt;
        }

        public static DataRow FillWith<T>(this DataRow dr, T item)
        {
            var props = typeof(T).GetProperties();
            foreach (var prop in props)
            {
                dr[prop.Name] = prop.GetValue(item);
            }
            return dr;
        }

        public static void AutoSqlBulkCopy(this DataTable dataTable, string connString)
        {
            var sqlConnection = new SqlConnection(connString);
            sqlConnection.Open();
            // checking whether the table selected from the dataset exists in the database or not
            var checkTableIfExistsCommand = new SqlCommand("IF EXISTS (SELECT 1 FROM sys.tables WHERE name =  '" 
                + dataTable.TableName.Split('.')[1] + "') SELECT 1 ELSE SELECT 0", sqlConnection);
            var exists = checkTableIfExistsCommand.ExecuteScalar().ToString().Equals("1");

            // if does not exist
            if (!exists)
            {
                var createTableBuilder = new StringBuilder("CREATE TABLE " + dataTable.TableName);
                createTableBuilder.AppendLine("(");

                // selecting each column of the datatable to create a table in the database
                foreach (DataColumn dc in dataTable.Columns)
                {
                    createTableBuilder.AppendLine("  [" + dc.ColumnName + $"] {GetSqlType(dc.DataType)},");
                }

                createTableBuilder.Remove(createTableBuilder.Length - 1, 1);
                createTableBuilder.AppendLine(")");

                var createTableCommand = new SqlCommand(createTableBuilder.ToString(), sqlConnection);
                createTableCommand.ExecuteNonQuery();
            }

            // if table exists, just copy the data to the destination table in the database
            // copying the data from datatable to database table
            using (var bulkCopy = new SqlBulkCopy(sqlConnection))
            {
                bulkCopy.DestinationTableName = dataTable.TableName;
                foreach (var column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ToString(), column.ToString());
                }
                bulkCopy.WriteToServer(dataTable);
            }
        }

        static string GetSqlType(Type dataTableColunmType)
        {
            //per type mappings here 
            //https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-type-mappings
            if (dataTableColunmType == typeof(string))
            {
                return "nvarchar(max)";
            }
            else if (dataTableColunmType == typeof(int))
            {
                return "int";
            }
            else if (dataTableColunmType == typeof(long))
            {
                return "bigint";
            }
            else if (dataTableColunmType == typeof(Single))
            {
                return "real";
            }
            else if (dataTableColunmType == typeof(double))
            {
                return "float";
            }
            else if (dataTableColunmType == typeof(DateTime))
            {
                return "datetime";
            }
            else if (dataTableColunmType == typeof(DateTimeOffset))
            {
                return "datetimeoffset";
            }
            else if (dataTableColunmType == typeof(byte[]))
            {
                return "varbinary(max)";
            }
            else
            {
                throw new NotSupportedException($"Type {dataTableColunmType.Name} not supported");
            }


        }
    }


}