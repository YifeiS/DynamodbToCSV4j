package be.quodlibet.dynamodbtocsv4j;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Dries Horions <dries@quodlibet.be>
 */
public class d2csv
{

    public d2csv(JSONObject config)
    {
        // output all columns if left blank, otherwise output only columns that matches the regex in the list
        List<String> queriedColList = new ArrayList<>();
    //    Collections.addAll(queriedColList,"id", "name", "contactInfo.*");
        String timeStamp = "[" + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()) + "]";


        if (validateConfig(config))
        {
            try
            {
                BasicAWSCredentials awsCreds = new BasicAWSCredentials((String) config.get("accessKeyId"), (String) config.get("secretAccessKey"));
                AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard()
                        .withRegion(Regions.US_WEST_2)
                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds));
                AmazonDynamoDB client = clientBuilder.build();


                ScanResult result = null;
                Map<String,AttributeValue> lastEvaluatedKey = null;
                int loopThreshold = 100;  // assuming 1MB of raw data contains 200+ items, 100 loops will result in 20000+ items, which should be enough
                int batchCount = 0;
                boolean isFirstLine = true;
                boolean noHeaderYet = true;
                int dummyColNumber = -1;
                List<Integer> numberList = new ArrayList<>();
                List<Object> headerList = new ArrayList<>();
                Map<Object, Integer> headerIndexMap = new TreeMap<>();
                boolean isprecautiousWrite = true; // TODO: looks like the column order is different between each scan, set this to true until the random-shuffle is resolved
                do {
                    batchCount++;
                    System.out.printf("d2csv: working on batch #%d\n", batchCount);
                    ScanRequest scanRequest = new ScanRequest().withTableName((String) config.get("tableName"));
                    if (config.has("filterExpression"))
                    {
                        scanRequest.withFilterExpression((String) config.get("filterExpression"));
                    }
                    if (config.has("expressionAttributeValues"))
                    {
                        JSONArray evals = (JSONArray) config.get("expressionAttributeValues");
                        Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
                        for (int i = 0; i < evals.length(); i++)
                        {
                            JSONObject val = (JSONObject) evals.get(i);
                            String type = val.getString("type");
                            AttributeValue av = new AttributeValue();
                            switch (type)
                            {
                                case "N":
                                    av.withN(val.getString("value"));
                                    break;
                                case "S":
                                    av.withS(val.getString("value"));
                                    break;
                                default:
                                    //handle all non numeric as String
                                    av.withS(val.getString("value"));
                            }
                            expressionAttributeValues.put(val.getString("name"), av);
                        }
                        scanRequest.withExpressionAttributeValues(expressionAttributeValues);
                    }
                    if (config.has("expressionAttributeNames"))
                    {
                        JSONArray evals = (JSONArray) config.get("expressionAttributeNames");
                        Map<String, String> expressionAttributeNames = new HashMap<String, String>();
                        for (int i = 0; i < evals.length(); i++)
                        {
                            JSONObject val = (JSONObject) evals.get(i);
                            expressionAttributeNames.put(val.getString("name"), val.getString("value"));
                        }
                        scanRequest.withExpressionAttributeNames(expressionAttributeNames);
                    }
                    if (lastEvaluatedKey != null) {
                        scanRequest.setExclusiveStartKey(lastEvaluatedKey);
                    }
                    result = client.scan(scanRequest);
                    lastEvaluatedKey = result.getLastEvaluatedKey();
                    System.out.println(result.getLastEvaluatedKey());
                    if (result.getLastEvaluatedKey() == null) {
                        System.out.println("poi...");
                    } else {
                        System.out.println("POI!!!");
                    }
                    //A map to hold all unique columns that were found, and their index
                    HashMap<String, Integer> columnMap = new HashMap();
                    //A map to hold all records
                    List<HashMap<Integer, String>> recordList = new ArrayList();
                    for (Map<String, AttributeValue> item : result.getItems())
                    {
                        HashMap<Integer, String> record = new HashMap();
                        handleMap("", item, columnMap, record);
                        recordList.add(record);
                    }

                    CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");
                    if (config.has("delimiter"))
                    {
                        csvFileFormat = csvFileFormat.withDelimiter(((String) config.get("delimiter")).charAt(0));

                    }
                    if (config.has("quotechar"))
                    {
                        csvFileFormat = csvFileFormat.withQuote(((String) config.get("quotechar")).charAt(0));
                    }
                    if (config.has("nullstring"))
                    {
                        csvFileFormat = csvFileFormat.withNullString((String) config.get("nullstring"));
                    }

                    FileWriter fileWriter;
                    try
                    {
                        String fileName = (String) config.get("tableName") + ".csv";
                        if (config.has("outputfile"))
                        {
                            fileName = (String) config.get("outputfile");
                        }
                        fileName = timeStamp + fileName;
                        fileWriter = new FileWriter(fileName, true);
                        CSVPrinter csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
                        //Create a map of columns indexed by their column nr
                        List<Integer> cols = new ArrayList(columnMap.values());
                        Collections.sort(cols);
                        Map<Integer, String> invertedColumnMap = new HashMap();
                        for (String key : columnMap.keySet())
                        {
                            invertedColumnMap.put(columnMap.get(key), key);
                        }
                        List colList = new ArrayList();
                        for (Integer i : cols)
                        {
                            colList.add(invertedColumnMap.get(i));
                        }

                        if (isprecautiousWrite || numberList.size() != headerList.size() || numberList.size() == 0) {
                            List prevNumberList = new ArrayList(numberList);
                            List prevHeaderList = new ArrayList(headerList);
                            numberList.clear();
                            headerList.clear();
                            headerIndexMap.clear();
                            for (int i = 0; i < colList.size(); i++) {
                                if (isMatch(queriedColList, colList.get(i))) {
                                    numberList.add(i);
                                    headerList.add(colList.get(i));
                                    headerIndexMap.put(colList.get(i), i);
                                }
                            }
                            // Column consistency check
                            for (String queriedCol : queriedColList) {
                                if (headerIndexMap.get(queriedCol) == null) {
                                    headerIndexMap.put(queriedCol, dummyColNumber);
                                }
                            }
                            Iterator it = headerIndexMap.keySet().iterator();
                            numberList.clear();
                            headerList.clear();
                            while (it.hasNext()) {
                                headerList.add(it.next());
                            }
                            numberList.addAll(headerIndexMap.values());
//                            System.out.println("numberList gen'ed from map: " + numberList.toString());
//                            System.out.println("headerList gen'ed from map: " + headerList.toString());

                        }


                        //Write the headers
                        if (noHeaderYet)
                        {
                            if (!config.has("headers") || (config.has("headers") && config.getString("headers").equals("true")))
                            {
//                                System.out.println("I am here~~" + headerList);  // NOTE: was colList
                                csvFilePrinter.printRecord(headerList); // NOTE: was colList);
                            }
                            noHeaderYet = false;
                        }


                        //Write the records
                        for (HashMap<Integer, String> record : recordList)
                        {

                            List recList = new ArrayList();
                            if (isFirstLine) {
                                isFirstLine = false;
                                System.out.println("This is cols: " + cols);
                                System.out.println("This is numberList: " + numberList.toString());
                            }
                            for (Integer i : numberList) // NOTE: was cols)
                            {
                                if (record.containsKey(i) )
                                {
                                    if (record.get(i) != null & !record.get(i).isEmpty())
                                    {
                                        recList.add(record.get(i));
                                    }
                                    else
                                    {
                                        recList.add(null);
                                    }
                                }
                                else
                                {
                                    if (i == dummyColNumber) {
                                        recList.add("N/A");
                                    } else {
                                        recList.add(null);
                                    }
                                }
                            }
                            csvFilePrinter.printRecord(recList);
                        }
                        csvFilePrinter.flush();
                    }
                    catch (IOException ex)
                    {
                        Logger.getLogger(d2csv.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    //==========================start writing===============================
                } while(result.getLastEvaluatedKey() != null && batchCount <= loopThreshold);

                if (batchCount > loopThreshold) {
                    System.out.println("PopopoPoi!");
                }


            }
            catch (JSONException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
    }

    private boolean isMatch(List<String> queriedColList, Object o) {
        if (queriedColList.size() == 0) {
            return true;
        }
        String str = o.toString();
//        System.out.println("matching column name: " + str);
        for (String candidate: queriedColList) {
            if (Pattern.compile("^" + candidate + "$", Pattern.CASE_INSENSITIVE).matcher(str).find()) {
                return true;
            }
        }
        return false;
    }

    private void handleMap(String path, Map<String, AttributeValue> item, HashMap<String, Integer> columnMap, HashMap<Integer, String> record)
    {
        for (String key : item.keySet())
        {
            String keyName = key;
            if (!path.isEmpty())
            {
                keyName = path + "." + key;
            }

            String type = "";
            if (item.get(key).getS() != null )
            {
                type = "S";
            }
            if (item.get(key).getN() != null)
            {
                type = "N";
            }
            if (item.get(key).getM() != null )

            {
                type = "M";
            }
            if (item.get(key).getL() != null)
            {
                type = "L";
            }
            if (item.get(key).getBOOL() != null)
            {
                type = "B";
            }
            //Add as column if it's not a list or map
            if (!type.equals("M") & !type.equalsIgnoreCase("L") & !columnMap.containsKey(keyName))
            {
                //Add newly discovered column
                columnMap.put(keyName, columnMap.size());
            }

            switch (type)
            {
                case "S":
                    record.put(columnMap.get(keyName), item.get(key).getS());
                    break;
                case "N":
                    record.put(columnMap.get(keyName), item.get(key).getN());
                    break;
                case "B":
                    record.put(columnMap.get(keyName), item.get(key).getBOOL().toString());
                    break;
                case "M":
                    Map< String, AttributeValue> a = (Map< String, AttributeValue>) item.get(key).getM();
                    handleMap(keyName, a, columnMap, record);
                    break;
                case "L":
                    Map< String, AttributeValue> ml = new HashMap();
                    List<AttributeValue> l = item.get(key).getL();
                    for (AttributeValue v : l)
                    {
                        ml.put("" + ml.size(), v);
                    }
                    handleMap(keyName, ml, columnMap, record);
                    break;
                default:
                    //System.out.println(keyName + " : \t" + item.get(key));
            }
        }
    }
    private Boolean validateConfig(JSONObject config)
    {
        Boolean valid = true;
        if (!config.has("accessKeyId"))
        {
            System.out.println("config parameter 'accessKeyId' is missing.");
            valid = false;
        }
        if (!config.has("secretAccessKey"))
        {
            System.out.println("config parameter 'secretAccessKey' is missing.");
            valid = false;
        }
        if (!config.has("region"))
        {
            System.out.println("config parameter 'region' is missing.");
            valid = false;
        }
        if (!config.has("tableName"))
        {
            System.out.println("config parameter 'tableName' is missing.");
            valid = false;
        }
        return valid;
    }
}
