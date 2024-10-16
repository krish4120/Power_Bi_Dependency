using Microsoft.Identity.Client;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace power_bi_blog
{
    public class PowerBi_Table
    {
        // Properties with getters and setters
        public string REFERENCED_DATABASE { get; set; }
        public string REFERENCED_SCHEMA { get; set; }
        public string REFERENCED_OBJECT_NAME { get; set; }
        public string REFERENCED_OBJECT_DOMAIN { get; set; }
        public string REFERENCING_DATABASE { get; set; }
        public string REFERENCING_SCHEMA { get; set; }
        public string REFERENCING_OBJECT_NAME { get; set; }
        public string REFERENCING_OBJECT_DOMAIN { get; set; }
        public string DATA_SOURCE { get; set; }
        public string DATASET_ID { get; set; }
        public string GROUP_ID { get; set; }
    }
    public class ReportInformation
    {
        public string ReportName { get; set; }
        public string DatasetId { get; set; }
        public string DatasetName { get; set; }
    }
    public class QueryDefinition
    {
        public string Datasource { get; set; }
        public string Database { get; set; }
        public string Schema { get; set; }
        public string ObjectName { get; set; }
        public string ObjectDomain { get; set; }
    }
    public class Dataset_With_Group_Name
    {
        public string DATASET_ID { get; set; }
        public string DATASET_NAME { get; set; }
        public string GROUP_ID { get; set; }
        public string GROUP_NAME { get; set; }
    }

    class Program
    {
        private readonly string _connectionString;
        private readonly HttpClient _client;
        private static readonly string powerBiApiUrl = "https://api.powerbi.com/v1.0/myorg/groups/";
        private static List<Dataset_With_Group_Name> datasetDictionary = new List<Dataset_With_Group_Name>();
        private static List<PowerBi_Table> tables = new List<PowerBi_Table>();

        public Program(string accessToken, string connectionString)
        {
            _client = new HttpClient();
            _client.BaseAddress = new Uri(powerBiApiUrl);
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            _connectionString = connectionString;
        }
        private async Task<(string content, int statusCode, string errorMessage)> FetchResponseBodyAsync(string url)
        {
            try
            {
                var response = await _client.GetAsync(url);
                var statusCode = (int)response.StatusCode;
                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    return (content, statusCode, null); // No error message in case of success
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    return (null, statusCode, errorContent); // Error message as content
                }
            }
            catch (Exception ex)
            {
                return (null, 500, ex.Message); // Return status code 500 for exceptions
            }
        }
        private string EscapeSql(string value)
        {
            return value?.Replace("'", "''");
        }
        private async Task InsertTablesAsync(List<PowerBi_Table> tables)
        {
            using (var connection = new SnowflakeDbConnection(_connectionString))
            {
                await connection.OpenAsync();

                var sb = new StringBuilder();
                sb.Append("INSERT OVERWRITE INTO PowerBiTables_dependencies (REFERENCED_DATABASE, REFERENCED_SCHEMA, REFERENCED_OBJECT_NAME, REFERENCED_OBJECT_DOMAIN, REFERENCING_DATABASE, REFERENCING_SCHEMA, REFERENCING_OBJECT_NAME,REFERENCING_OBJECT_DOMAIN,DATA_SOURCE,DATASET_ID,GROUP_ID) VALUES ");

                for (int i = 0; i < tables.Count; i++)
                {
                    var table = tables[i];
                    sb.Append($"('{EscapeSql(table.REFERENCED_DATABASE)}', '{EscapeSql(table.REFERENCED_SCHEMA)}', '{EscapeSql(table.REFERENCED_OBJECT_NAME)}', '{EscapeSql(table.REFERENCED_OBJECT_DOMAIN)}', '{EscapeSql(table.REFERENCING_DATABASE)}', '{EscapeSql(table.REFERENCING_SCHEMA)}', '{EscapeSql(table.REFERENCING_OBJECT_NAME)}', '{EscapeSql(table.REFERENCING_OBJECT_DOMAIN)}',  '{EscapeSql(table.DATA_SOURCE)}', '{EscapeSql(table.DATASET_ID)}','{EscapeSql(table.GROUP_ID)}')");

                    if (i < tables.Count - 1)
                    {
                        sb.Append(", ");
                    }
                }
                var commandText = sb.ToString();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = commandText;
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        static async Task<string> GetAccessTokenAsync()
        {
            string clientId = "<Enter Your Client ID>";
            string clientSecret = "<Enter Your Client Secret>";
            string tenantId = "<Enter Your Tenant Id>";
            var app = ConfidentialClientApplicationBuilder.Create(clientId)
                 .WithClientSecret(clientSecret)
                 .WithAuthority(new Uri($"https://login.microsoftonline.com/{tenantId}"))
                 .Build();
            string[] scopes = { "https://analysis.windows.net/powerbi/api/.default" };
            AuthenticationResult result = await app.AcquireTokenForClient(scopes).ExecuteAsync();
            string newAccessToken = result.AccessToken;
            // Create a new JSON object for the access token
            Console.Write(newAccessToken);
            return newAccessToken;
        }

        private async Task GetDatasetsAsync(string groupId, string groupName)
        {
            var (responseBody, statusCode, errorMessage) = await FetchResponseBodyAsync($"{groupId}/datasets");
            // Ensure the response is not empty
            if (statusCode == 200)
            {
                var jsonObject = JObject.Parse(responseBody);
                var datasets = jsonObject["value"];
                foreach (var dataset in datasets)
                {
                    string datasetId = dataset["id"].ToString();
                    string datasetName = dataset["name"].ToString();
                    Dataset_With_Group_Name table = new Dataset_With_Group_Name
                    {
                        DATASET_ID = datasetId,
                        DATASET_NAME = datasetName,
                        GROUP_ID = groupId,
                        GROUP_NAME = groupName
                    };
                    datasetDictionary.Add(table);
                }
            }
            else
            {
                Console.WriteLine("Failed in Datasets");
                Console.WriteLine(errorMessage + "    " + statusCode);
            }
        }

        private async Task<JObject> ExecuteQueryAsync(string datasetId, string query)
        {
            string url = $"https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/executeQueries";

            // Define your query payload
            var payload = new
            {
                queries = new[]
                {
            new { query = query }
        },
                serializerSettings = new { includeNulls = true }
            };

            // Serialize the payload to JSON
            string jsonPayload = Newtonsoft.Json.JsonConvert.SerializeObject(payload);

            // Create the content to send with the POST request
            HttpContent content = new StringContent(jsonPayload);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            try
            {
                // Send the POST request
                HttpResponseMessage response = await _client.PostAsync(url, content);

                // Check if the request was successful
                if (response.IsSuccessStatusCode)
                {
                    // Parse and return the response as a JObject
                    string responseBody = await response.Content.ReadAsStringAsync();
                    return JObject.Parse(responseBody);
                }
                else
                {
                    // Log the error without throwing an exception
                    string errorResponse = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Failed to execute query. Status code: {response.StatusCode}. Error: {errorResponse}");

                    return new JObject();
                }
            }
            catch (HttpRequestException ex)
            {
                // Log the specific HttpRequestException without throwing it
                Console.WriteLine($"HttpRequestException: {ex.Message}");
                // Return an empty JObject or handle it as needed
                return new JObject();
            }
            catch (Exception ex)
            {
                // Log any other unexpected exceptions without throwing them
                Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                // Return an empty JObject or handle it as needed
                return new JObject();
            }
        }
        private static List<QueryDefinition> ExtractQueryDefinitions(JObject result)
        {
            var queryDefinitions = new List<QueryDefinition>();

            // Navigate to the "results" array
            JArray results = (JArray)result["results"];

            foreach (JObject resultItem in results)
            {
                // Navigate to the "tables" array
                JArray tables = (JArray)resultItem["tables"];

                foreach (JObject table in tables)
                {
                    // Navigate to the "rows" array
                    JArray rows = (JArray)table["rows"];
                    foreach (JObject row in rows)
                    {
                        // Extract the Query field
                        if (row.TryGetValue("[Query]", out JToken queryToken))
                        {
                            string queryDefinition = queryToken.ToString();
                            string datasourcePattern = @"(\w+)\.\Databases";
                            string databasePattern = @"Name\s*=\s*#?""?([^,""]+)""?\s*,\s*Kind\s*=\s*""Database""";
                            string schemaPattern = @"Database\{\[Name=""([^""]+)"",Kind=""Schema""\]\}\[Data\]";
                            string objectPattern = @"_Schema\{\[Name=""([^""]+)"",Kind=""(Table|View)""\]\}\[Data\]";
                            string objectDomainPattern = @"Kind=""(Table|View)""";
                            string sql_databasePattern = @"Sql\.Database\([^,""]+,\s*""([^""]+)""";
                            string sql_schemaPattern = @"Schema\s*=\s*""([^""]+)""";
                            string sql_objectPattern = @"Item\s*=\s*""([^""]+)""";

                            // Create regex objects
                            Regex datasourceRegex = new Regex(datasourcePattern);
                            Regex databaseRegex = new Regex(databasePattern);
                            Regex schemaRegex = new Regex(schemaPattern);
                            Regex objectRegex = new Regex(objectPattern);
                            Regex objectDomainRegex = new Regex(objectDomainPattern);
                            Regex sql_databaseRegex = new Regex(sql_databasePattern);
                            Regex sql_schemaRegex = new Regex(sql_schemaPattern);
                            Regex sql_objectRegex = new Regex(sql_objectPattern);

                            // Extract matches
                            var datasourceMatch = datasourceRegex.Match(queryDefinition);
                            var databaseMatch = databaseRegex.Match(queryDefinition);
                            var schemaMatch = schemaRegex.Match(queryDefinition);
                            var objectMatch = objectRegex.Match(queryDefinition);
                            var sql_databaseMatch = sql_databaseRegex.Match(queryDefinition);
                            var sql_schemaMatch = sql_schemaRegex.Match(queryDefinition);
                            var sql_objectMatch = sql_objectRegex.Match(queryDefinition);
                            // Create and populate QueryDefinition object
                            var queryDef = new QueryDefinition
                            {
                                Datasource = datasourceMatch.Success ? datasourceMatch.Groups[1].Value : null,
                                Database = databaseMatch.Success ? databaseMatch.Groups[1].Value : (sql_databaseMatch.Success ? sql_databaseMatch.Groups[1].Value : null),
                                Schema = schemaMatch.Success ? schemaMatch.Groups[1].Value : (sql_schemaMatch.Success ? sql_schemaMatch.Groups[1].Value : null),
                                ObjectName = objectMatch.Success ? objectMatch.Groups[1].Value : (sql_objectMatch.Success ? sql_objectMatch.Groups[1].Value : null),
                                ObjectDomain = objectMatch.Success ? objectMatch.Groups[2].Value : null
                            };
                            queryDefinitions.Add(queryDef);
                        }
                    }
                }
            }
            return queryDefinitions;
        }

        private async Task<(string content, int statusCode, string errorMessage)> GetDatasetNameAsync(string groupId, string datasetId)
        {
            var (responseBody, statusCode, errorMessage) = await FetchResponseBodyAsync($"{groupId}/datasets/{datasetId}");
            if (statusCode == 200)
            {
                JObject json = JObject.Parse(responseBody);
                string datasetName = json["name"].ToString();

                return (datasetName, statusCode, errorMessage);
            }
            else
            {

                return (string.Empty, statusCode, errorMessage);
            }
        }
        private async Task<object> GetReportsAsync(string groupId, string groupName, string datasetId, string datasetName)
        {
            var (responseBody, statusCode, errorMessage) = await FetchResponseBodyAsync($"{groupId}/reports");
            if (statusCode == 200)
            {
                var jsonObject = JObject.Parse(responseBody);
                List<ReportInformation> reports = new List<ReportInformation>();
                foreach (JObject report in jsonObject["value"])
                {
                    try
                    {
                        reports.Add(new ReportInformation
                        {
                            ReportName = report["name"].ToString(),
                            DatasetId = datasetId,
                            DatasetName = datasetName
                        }
                        );
                    }
                    catch (Exception ex)
                    {
                        // Handle any issues that occur while getting the dataset name
                        Console.WriteLine($"Error retrieving dataset name for report {report["name"]}: {ex.Message}");
                    }
                }
                return reports;
            }
            else
            {
                return new List<ReportInformation>();
            }
        }

        static async Task Main(string[] args)
        {
            string query = @"EVALUATE
                                    SELECTCOLUMNS(
                                                  INFO.PARTITIONS(),
                                                  ""Query"", [QueryDefinition])";
            var connectionString = "scheme=https;ACCOUNT=<your_account>;HOST=<your_host>.central-india.azure.snowflakecomputing.com;port=<your_port>;user=<your_username>;password=<your_password>;db=<your_database>;SCHEMA=<your_schema>;warehouse=<your_warehouse>;role=<your_role>";

            var accessToken = await GetAccessTokenAsync();
            var obj = new Program(accessToken, connectionString);
            var group_id = "<Your Group Id>";
            var group_name = "<Your Workspace>";
            await obj.GetDatasetsAsync(group_id, group_name);

            foreach (var dataset in datasetDictionary)
            {
                List<ReportInformation> reports = (List<ReportInformation>)await obj.GetReportsAsync(group_id, group_name, dataset.DATASET_ID, dataset.DATASET_NAME);
                foreach (var report in reports)
                {
                    string report_name = report.ReportName;
                    string dataset_name = report.DatasetName;
                    PowerBi_Table table = new PowerBi_Table
                    {
                        REFERENCED_DATABASE = "PROD",
                        REFERENCED_SCHEMA = group_name,
                        REFERENCED_OBJECT_NAME = dataset_name,
                        REFERENCED_OBJECT_DOMAIN = "POWER BI DATASET",
                        REFERENCING_DATABASE = "PROD",
                        REFERENCING_SCHEMA = group_name,
                        REFERENCING_OBJECT_NAME = report_name,
                        REFERENCING_OBJECT_DOMAIN = "POWER BI REPORT",
                        DATA_SOURCE = "POWER_BI",
                        DATASET_ID = "-",
                        GROUP_ID = "-"
                    };
                    tables.Add(table);
                }
                JObject result_json = await obj.ExecuteQueryAsync(dataset.DATASET_ID, query);
                bool hasProperties = result_json.Properties().Any();

                if (hasProperties)
                {
                    List<QueryDefinition> definitions = ExtractQueryDefinitions(result_json);
                    foreach (var def in definitions)
                    {
                        string datasource = def.Datasource;
                        string database = def.Database;
                        string schema = def.Schema;
                        string object_name = def.ObjectName;
                        string object_domain = def.ObjectDomain;

                        if (!string.IsNullOrEmpty(datasource) && !string.IsNullOrEmpty(schema) &&
                            !string.IsNullOrEmpty(object_name) && !string.IsNullOrEmpty(object_domain))
                        {
                            PowerBi_Table table = new PowerBi_Table
                            {
                                REFERENCED_DATABASE = database,
                                REFERENCED_SCHEMA = schema,
                                REFERENCED_OBJECT_NAME = object_name,
                                REFERENCED_OBJECT_DOMAIN = object_domain,
                                REFERENCING_DATABASE = "PROD",
                                REFERENCING_SCHEMA = dataset.GROUP_NAME,
                                REFERENCING_OBJECT_NAME = dataset.DATASET_NAME,
                                REFERENCING_OBJECT_DOMAIN = "POWER BI DATASET",
                                DATA_SOURCE = datasource,
                                DATASET_ID = dataset.DATASET_ID,
                                GROUP_ID = dataset.GROUP_ID
                            };
                            tables.Add(table);
                        }
                    }
                }

            }
            await obj.InsertTablesAsync(tables);
            Console.WriteLine("Records are inserted Successfully");
        }
    }
}