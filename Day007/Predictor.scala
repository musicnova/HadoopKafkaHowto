package ru.croc.smartdata.spark;

import java.util.Map;

import static ru.croc.smartdata.spark.GroovyPredictor.SCRIPT_SUCCESS;

public interface Predictor {
    int RESULT_SUCCESS = SCRIPT_SUCCESS;

    Integer run(Object o);

    /* Commented out to not enforce for backward compatibility */
    //Map<String, String> calculate(Object o);
}
