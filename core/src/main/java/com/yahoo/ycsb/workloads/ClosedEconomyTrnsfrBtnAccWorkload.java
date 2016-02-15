/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

/*
 *@author: https://github.com/akon-dey/YCSB/blob/master/core/src/main/java/com/yahoo/ycsb/workloads/ClosedEconomyTrnsfrBtnAccWorkload.java
 */
package com.yahoo.ycsb.workloads;

import java.util.Properties;

import com.yahoo.ycsb.*;
import static com.yahoo.ycsb.Workload.INSERT_START_PROPERTY;
import static com.yahoo.ycsb.Workload.INSERT_START_PROPERTY_DEFAULT;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD
 * operations. The relative proportion of different kinds of operations, and
 * other properties of the workload, are controlled by parameters specified at
 * runtime.
 *
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one
 * (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all
 * fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads
 * (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates
 * (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts
 * (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans
 * (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be
 * read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select
 * the records to operate on - uniform, zipfian, hotspot, or latest (default:
 * uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to
 * scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be
 * used to choose the number of records to scan, for each scan, between 1 and
 * maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key
 * ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul>
 */
public class ClosedEconomyTrnsfrBtnAccWorkload extends Workload
{

    /**
     * The name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY = "table";

    /**
     * The default name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

    public static String table;

    /**
     * The name of the property for the number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY = "fieldcount";

    /**
     * Default number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

    int fieldcount;

    ////public static final String STATUS_NAME_REWARD_CUSTOMER = "RewardCustomer"; //shegufta
    ////public static final String STATUS_NAME_PAY_TO_BANK = "PayToBank"; //shegufta
    public static final String STATUS_NAME_TRANSFER_BETWEEN_ACC = "TransferBetweenAcc"; //shegufta
    public static final String OPERATION_COUNT_PROPERTY = "operationcount";

    public static final String TRANSACTION_TRACE_ON = "printTransactionTrace";
    public static final String TRANSACTION_TRACE_ON_DEFAULT = "false";
    boolean printTransactionTrace;

    public static final String GENERATED_KEYS_IN_READ_OPERATION = "printKeysInReadOperation";
    public static final String GENERATED_KEYS_IN_READ_OPERATION_DEFAULT = "false";
    boolean printKeysInReadOperation;

    public static final String GENERATED_KEYS_IN_TRANSFER_OPERATION = "printKeysInTransferOperation";
    public static final String GENERATED_KEYS_IN_TRANSFER_OPERATION_DEFAULT = "false";
    boolean printKeysInTransferOperation;

    /**
     * The name of the property for the field length distribution. Options are
     * "uniform", "zipfian" (favoring short records), "constant", and
     * "histogram".
     *
     * If "uniform", "zipfian" or "constant", the maximum field length will be
     * that specified by the fieldlength property. If "histogram", then the
     * histogram will be read from the filename specified in the
     * "fieldlengthhistogram" property.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

    /**
     * The name of the property for the length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY = "fieldlength";
    public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

    /**
     * The name of the property for the total amount of money in the economy at
     * the start.
     */
    //public static final String TOTAL_CASH_PROPERTY = "total_cash"; //Shegufta:: I have disabled it
    /**
     * The default total amount of money in the economy at the start.
     */
    //public static final String TOTAL_CASH_PROPERTY_DEFAULT = "1000000"; // Shegufta:: I have commented it out
    /**
     * The name of the property for the initial amount of money in each account
     * (#of account is defined by RECORD_COUNT_PROPERTY) plus in the
     * bankACCOUNT; Hence the total cash will be "InitialAmount*(1+RecordCount)"
     */
    public static final String INITIAL_CASH_PROPERTY = "initialcash";//Shegufta:: I have added it... read the comment above
    public static final String INITIAL_CASH_PROPERTY_DEFAULT = "1000";//shegufta

    /**
     * The name of a property that specifies the filename containing the field
     * length histogram (only used if fieldlengthdistribution is "histogram").
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

    /**
     * Generator object that produces field lengths. The value of this depends
     * on the properties that start with "FIELD_LENGTH_".
     */
    IntegerGenerator fieldlengthgenerator;

    /**
     * The name of the property for deciding whether to read one field (false)
     * or all fields (true) of a record.
     */
    //public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";
    //public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";
    //boolean readallfields;
    /**
     * The name of the property for deciding whether to write one field (false)
     * or all fields (true) of a record.
     */
    //public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
    //public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";
    //boolean writeallfields;
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * The name of the property for the proportion of transactions where
     * customers give money to bank.
     */
    ////public static final String PAY_TO_BANK_PROPERTY = "paytobankproportion";
    ////public static final String PAY_TO_BANK_PROPERTY_DEFAULT = "0.25";
    /**
     * The name of the property for the proportion of transactions where bank
     * gives reward to its customer.
     */
    ////public static final String REWARD_CUSTOMER_PROPERTY = "rewardcustomerproportion";
    ////public static final String REWARD_CUSTOMER_PROPERTY_DEFAULT = "0.25";
    /**
     * Tx between acc
     */
    public static final String TRANS_BETWEEN_CUSTOMER_PROPERTY = "transferbetweencustomerproportion";
    public static final String TRANS_BETWEEN_CUSTOMER_PROPERTY_DEFAULT = "0.50";

    /**
     * The name of the property for the proportion of transactions that are
     * reads.
     */
    public static final String READ_PROPORTION_PROPERTY = "readproportion";
    public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.0"; // shegufta:: in Original YCSB code, the default value was 0.95

    /**
     * The name of the property for the proportion of transactions that are
     * scans.
     */
    ////public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";
    ////public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /*
     *@NOTE: shegufta:: in this ClosedEconomyTrnsfrBtnAccWorkload, we will not use update, insert or readmodifywrite... Hence they are commented out...
     */
    /**
     * The name of the property for the proportion of transactions that are
     * updates.
     */
    //public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";
    //public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.00"; // shegufta:: in Original YCSB code, the default value was 0.05
    /**
     * The name of the property for the proportion of transactions that are
     * inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion"; // shegufta:: it is required for the zipfandistribution...
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are
     * read-modify-write.
     */
    //public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";
    //public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * The name of the property for the the distribution of requests across the
     * keyspace. Options are "uniform", "zipfian" and "latest"
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

    /**
     * The default distribution of requests across the keyspace
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the max scan length (number of records)
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

    /**
     * The default max scan length.
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

    /**
     * The name of the property for the scan length distribution. Options are
     * "uniform" and "zipfian" (favoring short scans)
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

    /**
     * The default max scan length.
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the order to insert records. Options are
     * "ordered" or "hashed"
     */
    public static final String INSERT_ORDER_PROPERTY = "insertorder";

    /**
     * Default insert order.
     */
    public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

    /**
     * Percentage data items that constitute the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

    /**
     * Default value of the size of the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

    /**
     * Percentage operations that access the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

    /**
     * Default value of the percentage operations accessing the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

    private Measurements _measurements;
    private Hashtable<String, String> _operations = new Hashtable<String, String>()
    {
        {
            put("READ", "TX-READ");
            //put("UPDATE", "TX-UPDATE");
            //put("INSERT", "TX-INSERT");
            //put("SCAN", "TX-SCAN");
            //put("READMODIFYWRITE", "TX-READMODIFYWRITE");
            //put(STATUS_NAME_PAY_TO_BANK, "TX-" + STATUS_NAME_PAY_TO_BANK);
            //put(STATUS_NAME_REWARD_CUSTOMER, "TX-" + STATUS_NAME_REWARD_CUSTOMER);
            put(STATUS_NAME_TRANSFER_BETWEEN_ACC, "TX-" + STATUS_NAME_TRANSFER_BETWEEN_ACC);
        }
    };

    IntegerGenerator keysequence;

    IntegerGenerator validation_keysequence;

    DiscreteGenerator operationchooser;

    IntegerGenerator keychooser;

    Generator fieldchooser;

    CounterGenerator transactioninsertkeysequence;

    IntegerGenerator scanlength;

    boolean orderedinserts;

    int recordcount;
    int opcount;
    AtomicInteger actualopcount = new AtomicInteger(0);
    ////AtomicInteger bankACCOUNT = null;//Shegufta
    private int totalcash;  // shegufta: I am using this variable in a different way.
    //From now on, total cash will be initialCash*(totalRecordCount)....
    private int initialCash; // shegufta: initial cash amount for each record;
    //private int currenttotal;// shegufta:: it is not used in the main YCSB+T code, hence i have commented it out
    //private int currentcount;// shegufta:: it is not used in the main YCSB+T code, hence i have commented it out
    //private int initialvalue;// shegufta:: I have replaced it by initialCash

    protected static IntegerGenerator getFieldLengthGenerator(Properties p) throws WorkloadException
    {
        //int num_records = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY)); // shegufta:: I have commented out this line.
        //int total_cash = Integer.parseInt(p.getProperty(TOTAL_CASH_PROPERTY, TOTAL_CASH_PROPERTY_DEFAULT));// shegufta:: I have commented out this line.
        int initialCash = Integer.parseInt(p.getProperty(INITIAL_CASH_PROPERTY, INITIAL_CASH_PROPERTY_DEFAULT));//shegufta

        IntegerGenerator fieldlengthgenerator;
        String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
        String fieldlengthhistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
        if (fieldlengthdistribution.compareTo("constant") == 0) {
            fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
            //fieldlengthgenerator = new ConstantIntegerGenerator(total_cash / num_records);// shegufta:: I have commented out this line.
        } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
            fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
            //fieldlengthgenerator = new UniformIntegerGenerator(1, total_cash / num_records);// shegufta:: I have commented out this line.
        } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
            fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
        } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
            try {
                fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
            } catch (IOException e) {
                throw new WorkloadException("Couldn't read field length histogram file: " + fieldlengthhistogram, e);
            }
        } else {
            throw new WorkloadException("Unknown field length distribution \"" + fieldlengthdistribution + "\"");
        }
        return fieldlengthgenerator;
    }

    /*
     *@author: shegufta
     */
    public void printTrace(String str)
    {
        if (this.printTransactionTrace) {
            System.out.println(str);
        }
    }

    public void printReadKey(int keynum)
    {
        if (this.printKeysInReadOperation) {
            System.out.println("*** R\t" + keynum);
        }
    }

    public void printTransactionKey(int customerFirstAcc, int customerSecondAcc)
    {
        if (this.printKeysInTransferOperation) {
            System.out.println("*** T\t" + customerFirstAcc + "\t" + customerSecondAcc);
        }
    }

    /**
     * Initialize the scenario. Called once, in the main client thread, before
     * any operations are started.
     */
    public void init(Properties p) throws WorkloadException
    {
        table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
        fieldlengthgenerator = ClosedEconomyTrnsfrBtnAccWorkload.getFieldLengthGenerator(p);
        fieldcount = Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

        double readproportion = Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
        //double updateproportion = Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
        double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));// shegufta:: it is required for the zipfandistribution... otherwise it is NOT required for our current workload
        //double scanproportion = Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
        //double readmodifywriteproportion = Double.parseDouble(p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

        ////double paytobankproportion = Double.parseDouble(p.getProperty(PAY_TO_BANK_PROPERTY, PAY_TO_BANK_PROPERTY_DEFAULT));//shegufta
        ////double rewardcustomerproportion = Double.parseDouble(p.getProperty(REWARD_CUSTOMER_PROPERTY, REWARD_CUSTOMER_PROPERTY_DEFAULT));//shegufta
        double transferbetweencustomerproportion = Double.parseDouble(p.getProperty(TRANS_BETWEEN_CUSTOMER_PROPERTY, TRANS_BETWEEN_CUSTOMER_PROPERTY_DEFAULT));//shegufta

        this.printTransactionTrace = Boolean.parseBoolean(p.getProperty(TRANSACTION_TRACE_ON, TRANSACTION_TRACE_ON_DEFAULT));
        this.printKeysInReadOperation = Boolean.parseBoolean(p.getProperty(GENERATED_KEYS_IN_READ_OPERATION, GENERATED_KEYS_IN_READ_OPERATION_DEFAULT));
        this.printKeysInTransferOperation = Boolean.parseBoolean(p.getProperty(GENERATED_KEYS_IN_TRANSFER_OPERATION, GENERATED_KEYS_IN_TRANSFER_OPERATION_DEFAULT));

        opcount = Integer.parseInt(p.getProperty(OPERATION_COUNT_PROPERTY, "0"));

        if (p.containsKey(Client.RECORD_COUNT_PROPERTY))// added by shegufta
        {
            recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
        } else {
            System.out.println("\n\nERROR: the property file does not contain " + Client.RECORD_COUNT_PROPERTY);
            System.out.println("inside ClosedEconomyTrnsfrBtnAccWorkload.java:: public void init(Properties p)");
            System.exit(1);
        }

        this.initialCash = Integer.parseInt(p.getProperty(INITIAL_CASH_PROPERTY, INITIAL_CASH_PROPERTY_DEFAULT));//shegufta

        int maxPossibleInitialCash = Integer.MAX_VALUE / (recordcount);
        if (maxPossibleInitialCash < this.initialCash) {//shegufta :: check for maximum possible initial cash
            System.out.println("\n\n\nERROR: initialCash = " + initialCash + " exceeds the maxPossibleInitialCash which is " + maxPossibleInitialCash);
            System.out.println("while calculating the totalcash = initialCash * (recordcount), totalcash will overflow ( it is an integer)");
            System.out.println("Here recordcount = " + recordcount);
            System.exit(1);
        }

        this.totalcash = initialCash * (recordcount);
        ////this.bankACCOUNT = new AtomicInteger(this.initialCash);//Shegufta

        System.out.println("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("opcount = " + opcount);
        System.out.println("recordcount = " + recordcount + "\n");
        System.out.println("INITIAL CASH: per account = " + initialCash);
        System.out.println("this.totalcash = initialCash * (recordcount) = " + this.totalcash);
        System.out.println("\nisTransactionTraceOn = " + printTransactionTrace);
        System.out.println("\nprintKeysInReadOperation = " + printKeysInReadOperation);
        System.out.println("\nprintKeysInTransferOperation = " + printKeysInTransferOperation);
        System.out.println("\nOPERATION RATIO:");
        System.out.println("readproportion = " + readproportion);
        //System.out.println("updateproportion = " + updateproportion);
        //System.out.println("insertproportion = " + insertproportion);
        //System.out.println("scanproportion = " + scanproportion);
        //System.out.println("readmodifywriteproportion = " + readmodifywriteproportion);
        //System.out.println("paytobankproportion = " + paytobankproportion);
        //System.out.println("rewardcustomerproportion = " + rewardcustomerproportion);
        System.out.println("transferbetweencustomerproportion = " + transferbetweencustomerproportion);
        double totalOptRatio = readproportion + transferbetweencustomerproportion;
        //double totalOptRatio = readproportion + scanproportion;//updateproportion + insertproportion + readmodifywriteproportion;
        //totalOptRatio += paytobankproportion + rewardcustomerproportion + transferbetweencustomerproportion;

        System.out.println("totalOptRatio = " + totalOptRatio);
        if (1.0 < totalOptRatio) {
            System.out.println("\n\t\tWARNING: total ratio should be equals to 1.0\n");
        }
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");

        String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        int maxscanlength = Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
        String scanlengthdistrib = p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

        int insertstart = Integer.parseInt(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));

        //readallfields = Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
        //writeallfields = Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));
        if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
            orderedinserts = false;
        } else if (requestdistrib.compareTo("exponential") == 0) {
            double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY, ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
            double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY, ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
            keychooser = new ExponentialGenerator(percentile, recordcount * frac);
        } else {
            orderedinserts = true;
        }

        keysequence = new CounterGenerator(insertstart);
        validation_keysequence = new CounterGenerator(insertstart);
        operationchooser = new DiscreteGenerator();
        if (readproportion > 0.0) {
            operationchooser.addValue(readproportion, "READ");
        }

        if (transferbetweencustomerproportion > 0.0) {
            operationchooser.addValue(transferbetweencustomerproportion, STATUS_NAME_TRANSFER_BETWEEN_ACC);
        }

        transactioninsertkeysequence = new CounterGenerator(recordcount);
        if (requestdistrib.compareTo("uniform") == 0) {
            keychooser = new UniformIntegerGenerator(0, recordcount - 1);
        } else if (requestdistrib.compareTo("zipfian") == 0) {
            // it does this by generating a random "next key" in part by taking the modulus over the number of keys
            // if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
            // so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
            // of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
            // plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
            // just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled
            // zipfian generator
            int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
            int expectednewkeys = (int) (((double) opcount) * insertproportion * 2.0); // 2 is fudge factor
            keychooser = new ScrambledZipfianGenerator(recordcount + expectednewkeys);
        } else if (requestdistrib.compareTo("latest") == 0) {
            keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
        } else if (requestdistrib.equals("hotspot")) {
            double hotsetfraction = Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
            double hotopnfraction = Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
            keychooser = new HotspotIntegerGenerator(0, recordcount - 1, hotsetfraction, hotopnfraction);
        } else {
            throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
        }

        fieldchooser = new UniformIntegerGenerator(0, fieldcount - 1);

        if (scanlengthdistrib.compareTo("uniform") == 0) {
            scanlength = new UniformIntegerGenerator(1, maxscanlength);
        } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
            scanlength = new ZipfianGenerator(1, maxscanlength);
        } else {
            throw new WorkloadException("Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
        }

        _measurements = Measurements.getMeasurements();
    }

    public String buildKeyName(long keynum)
    {
//      if (!orderedinserts) {
//          keynum = Utils.hash(keynum);
//      }
        // System.err.println("key: " + keynum);
        return "user" + keynum;
    }

    HashMap<String, ByteIterator> buildValues()
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        String fieldkey = "field0";
        ByteIterator data = new StringByteIterator("" + this.initialCash);
        values.put(fieldkey, data);
        return values;
    }

    HashMap<String, ByteIterator> buildUpdate()
    {
        // update a random field
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        String fieldname = "field" + fieldchooser.nextString();

        //ByteIterator data = new RandomByteIterator(fieldlengthgenerator.nextInt());// AKON's line
        ByteIterator data = new StringByteIterator("" + fieldlengthgenerator.nextInt());// Shegufta B Ahsan :: I have replaced RandomByteIterator with StringByteIterator which is more appropriate for this situation. RandomByteIterator generates a random sequence which eventually results crash in closedEconomyWorkload scenario
        values.put(fieldname, data);
        return values;
    }

    /**
     * Do one insert operation. Because it will be called concurrently from
     * multiple client threads, this function must be thread safe. However,
     * avoid synchronized, or the threads will block waiting for each other, and
     * it will be difficult to reach the target throughput. Ideally, this
     * function would have no side effects other than DB operations.
     *
     * @throws WorkloadException
     */
    public boolean doInsert(DB db, Object threadstate) throws WorkloadException
    {
        int keynum = keysequence.nextInt();
        String dbkey = buildKeyName(keynum);
        HashMap<String, ByteIterator> values = buildValues();
        if (db.insert(table, dbkey, values).equals(Status.OK)) {
            actualopcount.addAndGet(1);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from
     * multiple client threads, this function must be thread safe. However,
     * avoid synchronized, or the threads will block waiting for each other, and
     * it will be difficult to reach the target throughput. Ideally, this
     * function would have no side effects other than DB operations.
     *
     * @throws WorkloadException
     */
    public boolean doTransaction(DB db, Object threadstate)
            throws WorkloadException
    {
        /*
         Shegufta B Ahsan::
         I am planning to change the work flow.
        
         1) There wont be any delete account operation ! All operations will be performed on the initially created account
         2) Definitely there will be INSERT operation to create account.
         3) The READ operation will be as it is before
         4) There wont be any DELETE operation
         5) There wont be any explicit UPDATE operation
         6) Unlinke my other workload, in this workload there wont be any bank account. There will be only
         customer account and balances will betransferred between two customer account. Hence there is only
         a single operation to do this "doTransactionTransferBetweenCustomer(db)"
         9) Right now the program takes the amount of TotalCash as an input and equally divide it among all the 'n' records.
         because of the integer variables, we might loose some numbers while dividing.
         So why not think it from the opposite way?
         Now we will rather take InitialCash as an input instead of TotalCash.
         We know the total number of account.
         Hence, now the TotalCash will be  {(n)*InitialCash}
         In this process we wont loose any number because of division!
         */

        Status transactionStatus = Status.TRANSACTION_IN_PROGRESS;
        String op = operationchooser.nextString();

        long st_microSec = System.nanoTime() / 1000;

        if (op.compareTo("READ") == 0) {
            transactionStatus = doTransactionRead(db);
        } else if (op.compareTo(STATUS_NAME_TRANSFER_BETWEEN_ACC) == 0) {
            transactionStatus = doTransactionTransferBetweenCustomer(db);
        } else {
            System.out.println("\n\tERROR: unknown operation");
            System.exit(1);
        }
        long en_microSec = System.nanoTime() / 1000;

        if (null == _operations.get(op)) {
            System.out.println("\n\toperation " + op + " not inserted in _operations");
            System.exit(1);
        }
        _measurements.measure(_operations.get(op), (int) ((en_microSec - st_microSec)));
        _measurements.reportStatus(_operations.get(op), transactionStatus);

        actualopcount.addAndGet(1);

        if (transactionStatus.equals(Status.OK)) {
            return true;
        } else {
            return false;
        }

    }

    int nextKeynum()
    {
        int keynum;
        if (keychooser instanceof ExponentialGenerator) {
            do {
                keynum = transactioninsertkeysequence.lastInt()
                        - keychooser.nextInt();
            } while (keynum < 0);
        } else {
            do {
                keynum = keychooser.nextInt();
            } while (keynum > transactioninsertkeysequence.lastInt());
        }
        return keynum;
    }

    public Status doTransactionRead(DB db)
    {
        // choose a random key
        int keynum = nextKeynum();
        String keyname = buildKeyName(keynum);

        this.printReadKey(keynum);

        String fieldname = "field0"; //+ fieldchooser.nextString(); //shegufta:: here we will alwys use field0... so there is no need to call fieldchooser !
        HashSet<String> fields = new HashSet<String>();
        fields.add(fieldname);

        HashMap<String, ByteIterator> firstvalues = new HashMap<String, ByteIterator>();

        return db.read(table, keyname, fields, firstvalues);
    }

    /*
     *@author: shegufta
     *transfer money from one customer account to another one.
     */
    public Status doTransactionTransferBetweenCustomer(DB db)
    {
        //@TODO:: do we need to disable autocommit and then explicitely call commit?

        String operationName = STATUS_NAME_TRANSFER_BETWEEN_ACC;
        printTrace("\n-state START -op " + operationName);

        int customerFirstAcc = nextKeynum();
        int customerSecondAcc = nextKeynum();

        int loopBreaker = Integer.MAX_VALUE;
        while (customerFirstAcc == customerSecondAcc) {
            customerSecondAcc = nextKeynum();
            loopBreaker--;

            if (0 == loopBreaker) {
                System.out.println("\t Cannot find 2nd unique account after " + Integer.MAX_VALUE + "th attempt... May be there is only one account !");
                System.exit(1);
            }
        }

        this.printTransactionKey(customerFirstAcc, customerSecondAcc);

        String customerFirstKey = buildKeyName(customerFirstAcc);
        String customerSecondKey = buildKeyName(customerSecondAcc);

        String fieldname = "field0";
        HashSet<String> fields = new HashSet<String>();
        fields.add(fieldname);

        HashMap<String, ByteIterator> customerFirstValues = new HashMap<String, ByteIterator>();
        HashMap<String, ByteIterator> customerSecondValues = new HashMap<String, ByteIterator>();

        Status transactionStatus = Status.TRANSACTION_IN_PROGRESS;

        try {
            // do the transaction

            db.start();

            if (db.read(table, customerFirstKey, fields, customerFirstValues).equals(Status.OK) && db.read(table, customerSecondKey, fields, customerSecondValues).equals(Status.OK)) {
                int firstCustomerBalance = Integer.parseInt(customerFirstValues.get("field0").toString());
                int secondCustomerBalance = Integer.parseInt(customerSecondValues.get("field0").toString());

                String from = "NOT INITIALIZED";
                String to = "NOT INITIALIZED";
                int amountOfTo = -1234;
                int amountOfFrom = -1234;

                if (0 < firstCustomerBalance) {
                    firstCustomerBalance--;
                    secondCustomerBalance++;

                    to = customerSecondKey;
                    from = customerFirstKey;
                    amountOfTo = secondCustomerBalance;
                    amountOfFrom = firstCustomerBalance;

                } else if (0 < secondCustomerBalance) {

                    firstCustomerBalance++;
                    secondCustomerBalance--;

                    to = customerFirstKey;
                    from = customerSecondKey;
                    amountOfTo = firstCustomerBalance;
                    amountOfFrom = secondCustomerBalance;

                } else {// both of them are bankrupt !
                    //_measurements.reportStatus(ClosedEconomyTrnsfrBtnAccWorkload.STATUS_NAME_TRANSFER_BETWEEN_ACC, Status.UNEXPECTED_STATE);
                    printTrace("-state END -success ERROR -op " + operationName + "\n");

                    transactionStatus = Status.INSUFFICIENT_BALANCE;
                }

                if (transactionStatus.equals(Status.TRANSACTION_IN_PROGRESS)) {

                    customerFirstValues.clear();
                    customerFirstValues.put("field0", new StringByteIterator(Integer.toString(firstCustomerBalance)));
                    customerSecondValues.clear();
                    customerSecondValues.put("field0", new StringByteIterator(Integer.toString(secondCustomerBalance)));

                    if ((db.update(table, customerFirstKey, customerFirstValues).equals(Status.OK)) && (db.update(table, customerSecondKey, customerSecondValues).equals(Status.OK))) {
                        printTrace("transfer -from " + from + " -to " + to);
                        printTrace("current money -from $" + amountOfFrom + " -to $" + amountOfTo);
                        printTrace("-state END -success OK -op " + operationName + "\n");
                        transactionStatus = Status.OK;
                    } else {
                        transactionStatus = Status.ERROR_WHILE_UPDATING;
                    }
                }

            } else {
                printTrace("-state END -success ERROR -op " + operationName + "\n");
                transactionStatus = Status.ERROR_WHILE_READING;
            }

        } catch (Exception e) {
            System.out.println("\n" + e.toString() + "\n");
            System.out.println("134aqw351 Inside ClosedEconomyTrnsfrBtnAccWorkload.java");
            System.exit(1);
        } finally {
            try {
                if (transactionStatus.equals(Status.OK)) {
                    db.commit();
                } else {
                    db.abort();
                }
            } catch (DBException ex) {
                transactionStatus = Status.UNEXPECTED_STATE;
            }
        }

        return transactionStatus;
    }

    public boolean doTransactionInsert(DB db)
    {
        // choose the next key
        int keynum = transactioninsertkeysequence.nextInt();

        String dbkey = buildKeyName(keynum);

        HashMap<String, ByteIterator> values = buildValues();
        return (db.insert(table, dbkey, values).equals(Status.OK));
    }

    /**
     * Perform validation of the database db after the workload has executed.
     *
     * @return false if the workload left the database in an inconsistent state,
     * true if it is consistent.
     * @throws WorkloadException
     * @author Akon Dey
     */
    public boolean validate(DB db) throws WorkloadException
    {
        HashSet<String> fields = new HashSet<String>();
        fields.add("field0");

        System.out.println("\nValidating data");
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        int counted_sum = 0;//this.bankACCOUNT.get();

        try {
            for (int i = 0; i < recordcount; i++) {
                String keyname = buildKeyName(validation_keysequence.nextInt());
                try {
                    db.start();
                    db.read(table, keyname, fields, values);
                    db.commit();
                } catch (DBException e) {
                    throw new WorkloadException(e);
                }
                counted_sum += Integer.parseInt(values.get("field0").toString());
            }
        } catch (Exception ex) {
            System.out.println("\n\tUnexpected exception... The program will terminate... Trace and fix the reason of the exception... Best of luck !");
            ex.printStackTrace(System.out);
            System.exit(1);
        }

        System.out.println("-------------------------");
        System.out.println("[Initial TOTAL CASH], " + totalcash);
        System.out.println("[After Operation, TOTAL COUNTED CASH], " + counted_sum);

        int count = actualopcount.intValue();
        System.out.println("[ACTUAL OPERATIONS], " + count);
        System.out.println("[ANOMALY SCORE], " + Math.abs((totalcash - counted_sum) / (1.0 * count)));
        System.out.println("-------------------------");

        if (counted_sum != totalcash) {
            System.out.println("Validation failed");
            System.out.println("-------------------------\n");
            return false;
        } else {
            System.out.println("Validation successful");
            System.out.println("-------------------------\n");
            return true;
        }

    }
}


/*
 #Sample Workload File
 # Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 #                                                                                                                                                                                 
 # Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 # may not use this file except in compliance with the License. You                                                                                                                
 # may obtain a copy of the License at                                                                                                                                             
 #                                                                                                                                                                                 
 # http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 #                                                                                                                                                                                 
 # Unless required by applicable law or agreed to in writing, software                                                                                                             
 # distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 # implied. See the License for the specific language governing                                                                                                                    
 # permissions and limitations under the License. See accompanying                                                                                                                 
 # LICENSE file.                                                                                                                                                                   


 # Yahoo! Cloud System Benchmark
 # Workload A: Update heavy workload
 #   Application example: Session store recording recent actions
 #                        
 #   Read/update ratio: 50/50
 #   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
 #   Request distribution: zipfian

 fieldcount=1
 threadcount = 1

 printTransactionTrace = false
 printKeysInReadOperation = false
 printKeysInTransferOperation = false

 recordcount=100
 operationcount=1000
 workload=com.yahoo.ycsb.workloads.ClosedEconomyTrnsfrBtnAccWorkload

 initialcash=2000

 requestdistribution=zipfian
 #requestdistribution=uniform

 transferbetweencustomerproportion=0.0
 readproportion=1.0

 #paytobankproportion=0.34
 #rewardcustomerproportion=0.33
 #updateproportion=0.0
 #scanproportion=0
 #insertproportion=0

 */
