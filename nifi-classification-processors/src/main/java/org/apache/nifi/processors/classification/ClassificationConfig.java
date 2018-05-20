package org.apache.nifi.processors.classification;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 *
 */
public class ClassificationConfig implements Serializable {

    private String classification;
    private String strategy;
    private String value;
    private boolean ignoreCase;
    private Object evalObject;

    @JsonProperty("classification")
    public String getClassification(){
        return classification;
    }

    @JsonProperty("classification")
    public void setClassification(String classification){
       this.classification = classification;
    }

    @JsonProperty("strategy")
    public String getStrategy(){
        return strategy;
    }

    @JsonProperty("strategy")
    public void setStrategy(String strategy){
        this.strategy = strategy;
    }

    @JsonProperty("value")
    public String getExpressionValue(){
        return value;
    }

    @JsonProperty("value")
    public void setExpressionValue(String value){
        this.value = value;
    }

    @JsonProperty("ignore_case")
    public boolean getIgnoreCase(){
        return ignoreCase;
    }

    @JsonProperty("ignore_case")
    public void setIgnoreCase(boolean ignoreCase){
        this.ignoreCase = ignoreCase;
    }

    public Object getEvalObject(){
        return evalObject;
    }

    public void setEvalObject(Object evalObject){
        this.evalObject = evalObject;
    }


    public ClassificationConfig(String classification, String strategy, String value) {
        setClassification(classification);
        setStrategy(strategy);
        setExpressionValue(value);
        setIgnoreCase(false);
        setEvalObject(null);
    }

    public ClassificationConfig(String classification, String strategy, String value, boolean ignoreCase) {
        setClassification(classification);
        setStrategy(strategy);
        setExpressionValue(value);
        setIgnoreCase(ignoreCase);
    }

    public ClassificationConfig(){

    }
}
