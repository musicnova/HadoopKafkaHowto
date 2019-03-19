package ru.croc.smartdata.spark;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.time.temporal.ChronoUnit;


/**
 *
 */
public class PredictorApi {

    public static void hbasePut(String tableName, Put put) {
        Utils.putHbase(tableName, put);
    }

    public static void hbasePut(String tableName, byte[] row, byte[] cf, byte[] qualifier, byte[] value) {
        Utils.putHbase(tableName, row, cf, qualifier, value);
    }

    public static Result hbaseGet(String tableName, byte[] row) {
        return Utils.getHbase(tableName, row);
    }

    public static Result hbaseGet(String tableName, Get get) {
        return Utils.getHbase(tableName, get);
    }

    public static ResultScanner hbaseScan(String tableName, Scan scan) {
        return Utils.scanHbase(tableName, scan);
    }

    public static byte[] toBytes(String v) {
        return Bytes.toBytes(v);
    }

    public static byte[] toBytes(Integer v) {
        return Bytes.toBytes(v);
    }

    public static byte[] toBytes(Long v) {
        return Bytes.toBytes(v);
    }

    public static byte[] toBytes(BigDecimal v) {
        return Bytes.toBytes(v.toPlainString());
    }

    public static String fromBytes(byte[] v) {
        return Bytes.toString(v);
    }

    public static String toString(byte[] v) {
        return Bytes.toString(v);
    }

    public static Integer toInt(byte[] v) {
        return Bytes.toInt(v);
    }

    public static Long toLong(byte[] v) {
        return Bytes.toLong(v);
    }

    public static BigDecimal toBigDecimal(byte[] v) {
        return new BigDecimal(Bytes.toString(v));
    }

    public static void predictorEnqueue(String name, Object params) {
        Utils.enqueuePredictor(name, params);
    }

    public static void kafkaPut(String topic, Object params) {
        Utils.sendToKafka(topic, params);
    }

    public static long calcDaysFromToday(byte[] date) {
        return calcDaysFromToday(date, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    public static long calcDaysFromToday(byte[] date, String format) {
        return Utils.calcTimeFromToday(date, format, ChronoUnit.DAYS);
    }

    public static long calcTimeFromToday(byte[] date, java.time.temporal.ChronoUnit unit) {
        return Utils.calcTimeFromToday(date, "yyyy-MM-dd HH:mm:ss.SSS", unit);
    }

    public static long calcTimeFromToday(byte[] date, String format, java.time.temporal.ChronoUnit unit) {
        return Utils.calcTimeFromToday(date, format, unit);
    }

    public static byte[] extractValue(Result row, byte[] cf, byte[] q) {
        return Utils.extractValue(row, cf, q);
    }
}
