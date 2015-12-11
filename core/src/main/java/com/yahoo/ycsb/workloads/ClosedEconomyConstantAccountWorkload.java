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
 *@author: https://github.com/akon-dey/YCSB/blob/master/core/src/main/java/com/yahoo/ycsb/workloads/ClosedEconomyConstantAccountWorkload.java
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
public class ClosedEconomyConstantAccountWorkload extends Workload
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
    /**
     * The default field length distribution.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

    /**
     * The name of the property for the length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY = "fieldlength";
    /**
     * The default maximum length of a field in bytes.
     */
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
    public static final String INITIAL_CASH_PROPERTY = "initial_cash";//Shegufta:: I have added it... read the comment above
    /**
     * The default total amount of money in the economy at the start.
     */
    public static final String INITIAL_CASH_PROPERTY_DEFAULT = "1000";//shegufta

    public static final String STATUS_NAME_REWARD_CUSTOMER = "RewardCustomer"; //shegufta

    public static final String STATUS_NAME_PAY_TO_BANK = "PayToBank"; //shegufta

    public static final String STATUS_NAME_TRANSFER_BETWEEN_ACC = "TransferBetweenAcc"; //shegufta

    public static final String OPERATION_COUNT_PROPERTY = "operationcount";

    /**
     * The name of a property that specifies the filename containing the field
     * length histogram (only used if fieldlengthdistribution is "histogram").
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
    /**
     * The default filename containing a field length histogram.
     */
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
    public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

    /**
     * The default value for the readallfields property.
     */
    public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

    boolean readallfields;

    /**
     * The name of the property for deciding whether to write one field (false)
     * or all fields (true) of a record.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

    /**
     * The default value for the writeallfields property.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

    boolean writeallfields;

    /**
     * The name of the property for the proportion of transactions that are
     * reads.
     */
    public static final String READ_PROPORTION_PROPERTY = "readproportion";

    /**
     * The default proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

    /**
     * The name of the property for the proportion of transactions that are
     * updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

    /**
     * The default proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

    /**
     * The name of the property for the proportion of transactions that are
     * inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

    /**
     * The default proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are
     * scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are
     * read-modify-write.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

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
            put("UPDATE", "TX-UPDATE");
            put("INSERT", "TX-INSERT");
            put("SCAN", "TX-SCAN");
            put("READMODIFYWRITE", "TX-READMODIFYWRITE");
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
    AtomicInteger bankACCOUNT = null;//Shegufta
    private int totalcash;  // shegufta: I am using this variable in a different way.
    //From now on, total cash will be initialCash*(totalRecordCount+1)....
    //the 1 is added for the bankACCOUNT
    private int initialCash; // shegufta: initial cash amount for each record and also for the bankACCOUNT;
    //private int currenttotal;// shegufta:: it is not used in the main YCSB+T code, hence i have commented it out
    //private int currentcount; // shegufta:: it is not used in the main YCSB+T code, hence i have commented it out
    //private int initialvalue;// shegufta:: I have replaced it by initialCash

    protected static IntegerGenerator getFieldLengthGenerator(Properties p) throws WorkloadException
    {
        //int num_records = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY)); // shegufta:: I have commented out this line.
        //int total_cash = Integer.parseInt(p.getProperty(TOTAL_CASH_PROPERTY, TOTAL_CASH_PROPERTY_DEFAULT));// shegufta:: I have commented out this line.
        int initialCash = Integer.parseInt(p.getProperty(INITIAL_CASH_PROPERTY, INITIAL_CASH_PROPERTY_DEFAULT));//shegufta

        if (!p.contains(INITIAL_CASH_PROPERTY)) {
            System.out.println("\n\tWARNING:: INITIAL_CASH_PROPERTY not found... useing the default value.... initialCash = " + initialCash+"\n");
        }

        IntegerGenerator fieldlengthgenerator;
        String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
        String fieldlengthhistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
        if (fieldlengthdistribution.compareTo("constant") == 0) {
            fieldlengthgenerator = new ConstantIntegerGenerator(initialCash);
            //fieldlengthgenerator = new ConstantIntegerGenerator(total_cash / num_records);// shegufta:: I have commented out this line.
        } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
            fieldlengthgenerator = new UniformIntegerGenerator(1, initialCash);
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

    /**
     * Initialize the scenario. Called once, in the main client thread, before
     * any operations are started.
     */
    public void init(Properties p) throws WorkloadException
    {
        table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
        fieldlengthgenerator = ClosedEconomyConstantAccountWorkload.getFieldLengthGenerator(p);
        fieldcount = Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

        double readproportion = Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
        double updateproportion = Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
        double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
        double scanproportion = Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
        double readmodifywriteproportion = Double.parseDouble(p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

        opcount = Integer.parseInt(p.getProperty(OPERATION_COUNT_PROPERTY, "0"));

        if (p.containsKey(Client.RECORD_COUNT_PROPERTY))// added by shegufta
        {
            recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
        } else {
            System.out.println("the property file does not contain " + Client.RECORD_COUNT_PROPERTY);
            System.out.println("inside ClosedEconomyConstantAccountWorkload.java:: public void init(Properties p)");
            System.exit(1);
        }
        //totalcash = Integer.parseInt(p.getProperty(TOTAL_CASH_PROPERTY, TOTAL_CASH_PROPERTY_DEFAULT));
        //currenttotal = totalcash;// shegufta:: it is not used in the main YCSB+T code, hence i have commented it out
        //currentcount = recordcount;// shegufta:: it is not used in the main YCSB+T code, hence i have commented it out
        this.initialCash = Integer.parseInt(p.getProperty(INITIAL_CASH_PROPERTY, INITIAL_CASH_PROPERTY_DEFAULT));//shegufta

        this.bankACCOUNT = new AtomicInteger(this.initialCash);//Shegufta

        String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        int maxscanlength = Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
        String scanlengthdistrib = p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

        int insertstart = Integer.parseInt(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));

        readallfields = Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
        writeallfields = Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

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
        if (readproportion > 0) {
            operationchooser.addValue(readproportion, "READ");
        }

        if (updateproportion > 0) {
            operationchooser.addValue(updateproportion, "UPDATE");
        }

        if (insertproportion > 0) {
            operationchooser.addValue(insertproportion, "INSERT");
        }

        if (readmodifywriteproportion > 0) {
            operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
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
         6) In Acon's code, update is not doing as it is said in his paper. According to the paper,
         when deleting an account, all of its amount will be added to the Banks account (say at $BANK_ACC). When an UPDATE
         is performed, it will read an account ($ACC_n) deduct one dollar from $BANK_ACC, and add it with $ACC_n.
         The whole process was not implemented in the paper. Also I feel that, when we delete an account, its key needs
         to be deleted from the datastructure so that it wont be called for next operations. I might implement that scheme later,
         but right now just to keep things simple, I assume that there is no Delete operation!
         7) If I understand the code correctly, current YCSB+T code does not use doTransactionReadModifyWrite() at all.
         I am planning to use it in my code for:
         7a) Transfer between account: the idea is same as the original code
         7b) Pay bill to bank: $1 will be transferred from an account to bank's account. It will be added to the $BANK_ACC
         7c) Reward customer: Bank will reward a customer. it will transfer $1 from $BANK_ACC to its customer's account
         Initially these three operations will be equally probable.
         8) SCAN: i m not sure if we need it or not !  Lets think about it later :)
         9) Right now the program takes the amount of TotalCash as an input and equally divide it among all the 'n' records.
         because of the integer variables, we might loose some numbers while dividing.
         So why not think it from the opposite way?
         Now we will rather take InitialCash as an input instead of TotalCash.
         We know the total number of account. PLUS in our code we will assign some amount to the $BANK_ACC
         Hence, now the TotalCash will be  {(1+n)*InitialCash}
         In this process we wont loose any number because of division!
         */
        boolean ret = true;
        long st = System.nanoTime();

        String op = operationchooser.nextString();

        if (op.compareTo("READ") == 0) {
            ret = doTransactionRead(db);
        } else if (op.compareTo("UPDATE") == 0) {
            ret = doTransactionUpdate(db);
        } else if (op.compareTo("INSERT") == 0) {
            ret = doTransactionInsert(db);
        } else if (op.compareTo("SCAN") == 0) {
            ret = doTransactionScan(db);
        } else {
            ret = doTransactionReadModifyWrite(db);
        }

        long en = System.nanoTime();
        _measurements.measure(_operations.get(op), (int) ((en - st) / 1000));

        /*
         // original code from https://github.com/akon-dey/YCSB/blob/master/core/src/main/java/com/yahoo/ycsb/workloads/ClosedEconomyConstantAccountWorkload.java
         if (ret) {
         _measurements.reportReturnCode(_operations.get(op), -1);
         } else {
         _measurements.reportReturnCode(_operations.get(op), 0);
         }
         */
        /*
         * @NOTE :: Shegufta Bakht Ahsan
         * @NOTE :: above commented portion is the original code written by AKON.. It was written in 2014
         * @NOTE :: in 2015 YCSB, reportReturnCode has been replaced by reportStatus
         * @NOTE :: also, the new function takes a STATUS value
         * @NOTE :: I have replaced -1 with Status.OK, and 0 with Status.ERROR
         * @NOTE :: I am not sure whether it will work fine or not...
         * @NOTE :: I have to double check it !
         */
        /*
         //Shegufta B Ahsan :: I have commented out this part of Akon's code. This things are called
         //from the corresponding functions of DBWrapper.java... e.g. _measurements.reportStatus("INSERT", res);
         //hence, most probably it is redundent..... BUT im not 100% sure right now.... Needs more thorough checking and understanding of whats going on !
         if (ret) {
         _measurements.reportStatus(_operations.get(op), Status.OK);
         } else {
         _measurements.reportStatus(_operations.get(op), Status.ERROR);
         }
         */
        actualopcount.addAndGet(1);

        return ret;
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

    public boolean doTransactionRead(DB db)
    {
        // choose a random key
        int keynum = nextKeynum();

        String keyname = buildKeyName(keynum);

        HashSet<String> fields = null;

        if (!readallfields) {
            // read a random field
            String fieldname = "field" + fieldchooser.nextString();

            fields = new HashSet<String>();
            fields.add(fieldname);
        }

        HashMap<String, ByteIterator> firstvalues = new HashMap<String, ByteIterator>();

        return (db.read(table, keyname, fields, firstvalues).equals(Status.OK));
    }

    /*
     *@author: shegufta
     * A customer will be rewarded with money transferred from bank account (provided that there are sufficient money available in the bank account)
     */
    public boolean doTransactionRewardCustomer(DB db)
    {
        //@TODO:: do we need to disable autocommit and then explicitely call commit?

        if (bankACCOUNT.decrementAndGet() < 0) // decrement Bank account and check if it is greater than zero... if not, increment it
        {// Bank does not have sufficient money !
            bankACCOUNT.incrementAndGet();
            _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_REWARD_CUSTOMER, Status.UNEXPECTED_STATE);
            return false;
        }

        int customerAcc = nextKeynum();
        String customerKey = buildKeyName(customerAcc);
        HashMap<String, ByteIterator> customerValues = new HashMap<String, ByteIterator>();

        String fieldname = "field0"; //+ fieldchooser.nextString(); //shegufta:: here we will alwys use field0... so there is no need to call fieldchooser !
        HashSet<String> fields = new HashSet<String>();
        fields.add(fieldname);

        try {
            // do the transaction
            long st = System.currentTimeMillis();
            if (db.read(table, customerKey, fields, customerValues).equals(Status.OK)) {

                int increasedAmount = 1 + Integer.parseInt(customerValues.get("field0").toString());

                customerValues.put("field0", new StringByteIterator(Integer.toString(increasedAmount)));

                if (db.update(table, customerKey, customerValues).equals(Status.OK)) {
                    _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_REWARD_CUSTOMER, Status.OK);
                    long en = System.currentTimeMillis();
                    Measurements.getMeasurements().measure(ClosedEconomyConstantAccountWorkload.STATUS_NAME_REWARD_CUSTOMER, (int) (en - st));
                    return true;
                }
            }
        } catch (Exception e) {
            System.out.println("\n" + e.toString() + "\n");
        }

        bankACCOUNT.incrementAndGet();
        // if the code works perfectly, it should not reach this point...
        // it will reach this point only if there is something wrong
        // which means it is not able to perform the update
        // hence increment the bankACCOUNT to set it as it was before.
        _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_REWARD_CUSTOMER, Status.UNEXPECTED_STATE);
        return false;
    }

    /*
     *@author: shegufta
     *Customer will pay an amount to the bank account. Provided that customer has sufficient money in his account
     */
    public boolean doTransactionPayToBank(DB db)
    {
        //@TODO:: do we need to disable autocommit and then explicitely call commit?

        int customerAcc = nextKeynum();
        String customerKey = buildKeyName(customerAcc);
        HashMap<String, ByteIterator> customerValues = new HashMap<String, ByteIterator>();

        String fieldname = "field0"; //+ fieldchooser.nextString(); //shegufta:: here we will alwys use field0... so there is no need to call fieldchooser !
        HashSet<String> fields = new HashSet<String>();
        fields.add(fieldname);

        try {
            // do the transaction
            long st = System.currentTimeMillis();
            if (db.read(table, customerKey, fields, customerValues).equals(Status.OK)) {
                int decreasedAmount = Integer.parseInt(customerValues.get("field0").toString()) - 1;
                if (decreasedAmount < 0) {// client does not have enough money !
                    _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_PAY_TO_BANK, Status.UNEXPECTED_STATE);
                    return false;
                }

                customerValues.put("field0", new StringByteIterator(Integer.toString(decreasedAmount)));

                if (db.update(table, customerKey, customerValues).equals(Status.OK)) {
                    bankACCOUNT.incrementAndGet();// add the amount to bankACCOUNT
                    _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_PAY_TO_BANK, Status.OK);
                    long en = System.currentTimeMillis();
                    Measurements.getMeasurements().measure(ClosedEconomyConstantAccountWorkload.STATUS_NAME_PAY_TO_BANK, (int) (en - st));
                    return true;
                }

            }
        } catch (Exception e) {
            System.out.println("\n" + e.toString() + "\n");
        }

        // if the code works perfectly, it should not reach this point...
        // it will reach this point only if there is something wrong
        // which means it is not able to perform the read/update etc.
        // hence increment the bankACCOUNT to set it as it was before.
        _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_REWARD_CUSTOMER, Status.UNEXPECTED_STATE);
        return false;
    }

    /*
     *@author: shegufta
     *transfer money from one customer account to another one.
     */
    public boolean doTransactionTransferBetweenCustomer(DB db)
    {
        //@TODO:: do we need to disable autocommit and then explicitely call commit?

        // choose a random key
        int first = nextKeynum();
        int second = first;
        while (second == first) {
            second = nextKeynum();
        }
        /* it was done in the main YCSB+T code, but I dont think it is necessary right now.
         // We want to move money in one direction only, to ensure transactions
         // don't 'cancel' one-another out, i.e. T1: A -> B, and concurrently
         // T2: B -> A. Hence, we only transfer from higher accounts to lower
         // accounts.
         if (first < second) {
         int temp = first;
         first = second;
         second = temp;
         }
         */

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

        String customerFirstKey = buildKeyName(customerFirstAcc);
        String customerSecondKey = buildKeyName(customerSecondAcc);

        String fieldname = "field0"; //+ fieldchooser.nextString(); //shegufta:: here we will alwys use field0... so there is no need to call fieldchooser !
        HashSet<String> fields = new HashSet<String>();
        fields.add(fieldname);

        HashMap<String, ByteIterator> customerFirstValues = new HashMap<String, ByteIterator>();
        HashMap<String, ByteIterator> customerSecondValues = new HashMap<String, ByteIterator>();

        try {
            // do the transaction
            long st = System.currentTimeMillis();

            if (db.read(table, customerFirstKey, fields, customerFirstValues).equals(Status.OK) && db.read(table, customerSecondKey, fields, customerSecondValues).equals(Status.OK)) {
                int firstCustomerBalance = Integer.parseInt(customerFirstValues.get("field0").toString());
                int secondCustomerBalance = Integer.parseInt(customerSecondValues.get("field0").toString());

                if (0 < firstCustomerBalance) {
                    firstCustomerBalance--;
                    secondCustomerBalance++;
                } else if (0 < secondCustomerBalance) {
                    firstCustomerBalance++;
                    secondCustomerBalance--;
                } else {// both of them are bankrupt !
                    _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_TRANSFER_BETWEEN_ACC, Status.UNEXPECTED_STATE);
                    return false;
                }

                customerFirstValues.put("field0", new StringByteIterator(Integer.toString(firstCustomerBalance)));
                customerSecondValues.put("field0", new StringByteIterator(Integer.toString(secondCustomerBalance)));

                if ((db.update(table, customerFirstKey, customerFirstValues).equals(Status.OK)) && (db.update(table, customerSecondKey, customerSecondValues).equals(Status.OK))) {
                    _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_TRANSFER_BETWEEN_ACC, Status.OK);
                    long en = System.currentTimeMillis();
                    Measurements.getMeasurements().measure(ClosedEconomyConstantAccountWorkload.STATUS_NAME_TRANSFER_BETWEEN_ACC, (int) (en - st));

                    return true;
                }

            }

            // if the code works well, it should not reach this point...
            // it will reach this point only if there is something wrong
            // which means it is not able to perform the read/update etc.
            // hence increment the bankACCOUNT to set it as it was before.
            _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_TRANSFER_BETWEEN_ACC, Status.UNEXPECTED_STATE);
            return false;
        } catch (Exception e) {
            System.out.println("\n" + e.toString() + "\n");
        }

        // if the code works perfectly, it should not reach this point...
        // it will reach this point only if there is something wrong
        // which means it is not able to perform the read/update etc.
        // hence increment the bankACCOUNT to set it as it was before.
        _measurements.reportStatus(ClosedEconomyConstantAccountWorkload.STATUS_NAME_REWARD_CUSTOMER, Status.UNEXPECTED_STATE);
        return false;
    }

    public boolean doTransactionReadModifyWrite(DB db)
    {
        // choose a random key
        int first = nextKeynum();
        int second = first;
        while (second == first) {
            second = nextKeynum();
        }
        // We want to move money in one direction only, to ensure transactions
        // don't 'cancel' one-another out, i.e. T1: A -> B, and concurrently
        // T2: B -> A. Hence, we only transfer from higher accounts to lower
        // accounts.
        if (first < second) {
            int temp = first;
            first = second;
            second = temp;
        }

        String firstkey = buildKeyName(first);
        String secondkey = buildKeyName(second);

        HashSet<String> fields = null;

        if (!readallfields) {
            // read a random field
            String fieldname = "field" + fieldchooser.nextString();

            fields = new HashSet<String>();
            fields.add(fieldname);
        }

        HashMap<String, ByteIterator> firstvalues = new HashMap<String, ByteIterator>();
        HashMap<String, ByteIterator> secondvalues = new HashMap<String, ByteIterator>();

        // do the transaction
        long st = System.currentTimeMillis();

        if (db.read(table, firstkey, fields, firstvalues).equals(Status.OK) && db.read(table, secondkey, fields, secondvalues).equals(Status.OK)) {
            try {
                int firstamount = Integer.parseInt(firstvalues.get("field0")
                        .toString());
                int secondamount = Integer.parseInt(secondvalues.get("field0")
                        .toString());

                if (firstamount > 0) {
                    firstamount--;
                    secondamount++;
                }

                firstvalues.put("field0",
                        new StringByteIterator(Integer.toString(firstamount)));
                secondvalues.put("field0",
                        new StringByteIterator(Integer.toString(secondamount)));

                boolean isUpdateFirst = db.update(table, firstkey, firstvalues).equals(Status.OK);
                boolean isUpdateSecond = db.update(table, secondkey, secondvalues).equals(Status.OK);
                if ((!isUpdateFirst) || (!isUpdateSecond)) {
                    return false;
                }

                long en = System.currentTimeMillis();

                Measurements.getMeasurements().measure("READ-MODIFY-WRITE", (int) (en - st));
            } catch (NumberFormatException e) {
                return false;
            }
            return true;
        }
        return false;
    }

    public boolean doTransactionScan(DB db)
    {
        // choose a random key
        int keynum = nextKeynum();

        String startkeyname = buildKeyName(keynum);

        // choose a random scan length
        int len = scanlength.nextInt();

        HashSet<String> fields = null;

        if (!readallfields) {
            // read a random field
            String fieldname = "field" + fieldchooser.nextString();

            fields = new HashSet<String>();
            fields.add(fieldname);
        }

        return (db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>()).equals(Status.OK));
    }

    public boolean doTransactionUpdate(DB db)
    {
        // choose a random key
        int keynum = nextKeynum();

        String keyname = buildKeyName(keynum);

        HashMap<String, ByteIterator> values;

        if (writeallfields) {
            // new data for all the fields
            values = buildValues();
        } else {
            // update a random field
            values = buildUpdate();
        }

        return (db.update(table, keyname, values).equals(Status.OK));
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
        System.out.println("Validating data");
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        int counted_sum = 0;
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

        if (counted_sum != totalcash) {
            System.out.println("Validation failed");
            System.out.println("[TOTAL CASH], " + totalcash);
            System.out.println("[COUNTED CASH], " + counted_sum);
            int count = actualopcount.intValue();
            System.out.println("[ACTUAL OPERATIONS], " + count);
            System.out.println("[ANOMALY SCORE], " + Math.abs((totalcash - counted_sum) / (1.0 * count)));
            return false;
        } else {
            return true;
        }
    }
}
