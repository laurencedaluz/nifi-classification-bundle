package org.apache.nifi.processors.classification;

import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SubsetKeyMappingConfig implements Serializable {

    private String key;
    private ArrayList<String> classifications;

    @JsonProperty("key")
    public String getKey(){
        return key;
    }

    @JsonProperty("key")
    public void setKey(String key){
       this.key = key;
    }

    @JsonProperty("classifications")
    public List<String> getClassifications(){
        return classifications;
    }

    @JsonProperty("classifications")
    public void setClassifications(ArrayList<String> classifications){
        this.classifications = classifications;
    }


    public SubsetKeyMappingConfig(String key, ArrayList<String> classifications) {
        setKey(key);
        setClassifications(classifications);
    }

    public SubsetKeyMappingConfig(){

    }
}
