package ru.croc.smartdata.spark;

import groovy.lang.Closure;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class PredictorIdentComplex implements Predictor {
    @Override
    public Integer run(Object o) {
        try {
            Map<String, String> res = calculate(o);
            if (res.containsKey(getAttributeName())) {
                byte[] rowkey = PredictorApi.toBytes((String) o);
                byte[] cf = PredictorApi.toBytes("ident_complex_attribute_smuid");
                byte[] q = PredictorApi.toBytes(getAttributeName());
                PredictorApi.hbasePut("smuid_data", rowkey, cf, q, PredictorApi.toBytes(res.get(getAttributeName())));
            }
        } catch (RuntimeException e) {
            System.err.println(e);
        }

        return RESULT_SUCCESS;
    }

    public Map<String, String> calculate(Object o) {
        // rowkey = "<smuid>"
        byte[] rowkey = PredictorApi.toBytes((String) o);

        ArrayList<String> attrs = initAttributes();
        Map<String, Map<String, byte[]>> meta = loadMetadata(attrs);
        Map<String, Integer> topicPriority = loadPriorities(getAttributeName());

        HashMap<String, String> res = new HashMap<>();
        for (String topic : topicPriority.keySet()) {
            byte[] val = loadAttributeByTopic(rowkey, topic, meta);
            if (val != null) {
                res.put(getAttributeName(), PredictorApi.toString(val));
                return res;
            }
        }
        return res;
    }

    protected Map<String, Map<String, byte[]>> loadMetadata(ArrayList<String> attrs) {
        byte[] cf = PredictorApi.toBytes("basic_metadata");
        byte[] qTopic = PredictorApi.toBytes("topic");
        byte[] qCf = PredictorApi.toBytes("column_family");
        byte[] qEntity = PredictorApi.toBytes("entity");

        Map<String, Map<String, byte[]>> meta = new HashMap<>();
        for (String name : attrs) {
            if (!meta.containsKey(name)) {
                Get get = new Get(PredictorApi.toBytes(name));
                get.addColumn(cf, qTopic);
                get.addColumn(cf, qCf);
                get.addColumn(cf, qEntity);

                Result row = PredictorApi.hbaseGet("attribute_metadata", get);
                Map<String, byte[]> attrMeta = new HashMap<>();
                attrMeta.put("topic", PredictorApi.extractValue(row, cf, qTopic));
                attrMeta.put("column_family", PredictorApi.extractValue(row, cf, qCf));
                attrMeta.put("entity", PredictorApi.extractValue(row, cf, qEntity));
                meta.put(name, attrMeta);
            }
        }

        return meta;
    }

    protected Map<String, Integer> loadPriorities(String name) {
        Map<String, Integer> priority = new HashMap<>();

        Get get = new Get(PredictorApi.toBytes(name));
        Result row = PredictorApi.hbaseGet("services.prioritet_topic_ident_attributes", get);
        List<Cell> cells = row.listCells();
        for (Cell c : cells) {
            priority.put(PredictorApi.toString(CellUtil.cloneQualifier(c)), PredictorApi.toInt(CellUtil.cloneValue(c)));
        }

        Map<String, Integer> sortedP = new LinkedHashMap<>();
        priority.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed()).forEachOrdered(x -> sortedP.put(x.getKey(), x.getValue()));
        return sortedP;
    }

    protected byte[] loadAttributeByTopic(byte[] rowkey, String topic, Map<String, Map<String, byte[]>> meta) {
        byte[] name = null;
        Map<String, byte[]> selected = null;

        for (String attr : meta.keySet()) {
            Map<String, byte[]> attrMeta = meta.get(attr);
            if (topic.contentEquals(PredictorApi.toString(attrMeta.get("topic")))) {
                name = PredictorApi.toBytes(attr);
                selected = attrMeta;
                break;
            }

        }


        if (name != null) {
            Get get = new Get(rowkey);
            get.addColumn(selected.get("column_family"), name);
            Result row = PredictorApi.hbaseGet(PredictorApi.toString(selected.get("entity")), get);
            byte[] val = PredictorApi.extractValue(row, selected.get("column_family"), name);
            return (val != null && val.length > 0) ? val : null;
        }

        return null;
    }

    protected abstract String getAttributeName();

    protected abstract ArrayList<String> initAttributes();
}
