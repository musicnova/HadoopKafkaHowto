package ru.croc.smartdata.spark;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyCodeSource;
import groovy.lang.GroovyObject;

public class GroovyPredictor {

    private static final String CLASS_NAME_PREFIX = "ru.croc.smartdata.spark.";
    private final String SCRIPT_NAME;
    private static final String CODE_BASE = GroovyPredictor.class.getPackage().getName();
    private static final String METHOD_NAME = "run";
    public static final int SCRIPT_SUCCESS = 0;

    GroovyPredictor(String scriptName) {
        SCRIPT_NAME = scriptName;
    }

    public int process(String scriptCode, Object args) throws Exception {
        return (int)call(scriptCode, METHOD_NAME, args);
    }

    public Object call(String scriptCode, String methodName, Object args) throws Exception {
        ClassLoader th = this.getClass().getClassLoader();
        GroovyClassLoader loader = new GroovyClassLoader(th);
        Class clazz = loader.parseClass(new GroovyCodeSource(scriptCode, CLASS_NAME_PREFIX + SCRIPT_NAME, CODE_BASE));
        GroovyObject obj = (GroovyObject)clazz.newInstance();
        return obj.invokeMethod(methodName, args);
    }
}
