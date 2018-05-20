/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.classification;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.NLKBufferedReader;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


//TODO: Add Property Validators
//TODO: Add external config validators
//TODO: Additional Unit tests


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"attributes", "routing", "text", "regexp", "regex", "Regular Expression", "Expression Language", "csv", "filter", "logs", "delimited"})
@CapabilityDescription("Routes textual data based on a set of user-defined rules. Each line in an incoming FlowFile is compared against the values specified by user-defined Properties. "
        + "The mechanism by which the text is compared to these user-defined properties is defined by the 'Matching Strategy'. The data is then routed according to these rules, routing "
        + "each line of the text individually.")
@DynamicProperty(name = "Relationship Name", value = "value to match against", description = "Routes data that matches the value specified in the Dynamic Property Value to the "
        + "Relationship specified in the Dynamic Property Key.")
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's value")
@WritesAttributes({
        @WritesAttribute(attribute = "RouteText.Route", description = "The name of the relationship to which the FlowFile was routed."),
        @WritesAttribute(attribute = "RouteText.Group", description = "The value captured by all capturing groups in the 'Grouping Regular Expression' property. "
                + "If this property is not set or contains no capturing groups, this attribute will not be added.")
})
public class ClassifyTextWithMapping extends AbstractProcessor {

    public static final String ROUTE_ATTRIBUTE_KEY = "RouteText.Route";
    public static final String GROUP_ATTRIBUTE_KEY = "RouteText.Group";

    private static final String startsWithValue = "starts_with";
    private static final String endsWithValue = "ends_with";
    private static final String containsValue = "contains";
    private static final String equalsValue = "equals";
    private static final String matchesRegularExpressionValue = "matches_regex";
    private static final String containsRegularExpressionValue = "contains_regex";
    private static final String satisfiesExpression = "satisfies_expression";


    public static final AllowableValue CLASSIFY_SINGLE_TAG = new AllowableValue("CLASSIFY_SINGLE_TAG", "Single classification based on first match",
            "Once a match is hit in single mode, remaining Regular Expressions are not evaluated.");
    public static final AllowableValue CLASSIFY_MULTI_TAG = new AllowableValue("CLASSIFY_MULTI_TAG", "Multiple classifications based on all matches",
            "All Regular Expressions are evaluated and classifications are added as Attr.1, Attr.2, ...");

    public static final AllowableValue STARTS_WITH = new AllowableValue(startsWithValue, startsWithValue,
            "Match lines based on whether the line starts with the property value");
    public static final AllowableValue ENDS_WITH = new AllowableValue(endsWithValue, endsWithValue,
            "Match lines based on whether the line ends with the property value");
    public static final AllowableValue CONTAINS = new AllowableValue(containsValue, containsValue,
            "Match lines based on whether the line contains the property value");
    public static final AllowableValue EQUALS = new AllowableValue(equalsValue, equalsValue,
            "Match lines based on whether the line equals the property value");
    public static final AllowableValue MATCHES_REGULAR_EXPRESSION = new AllowableValue(matchesRegularExpressionValue, matchesRegularExpressionValue,
            "Match lines based on whether the line exactly matches the Regular Expression that is provided as the Property value");
    public static final AllowableValue CONTAINS_REGULAR_EXPRESSION = new AllowableValue(containsRegularExpressionValue, containsRegularExpressionValue,
            "Match lines based on whether the line contains some text that matches the Regular Expression that is provided as the Property value");
    public static final AllowableValue SATISFIES_EXPRESSION = new AllowableValue(satisfiesExpression, satisfiesExpression,
            "Match lines based on whether or not the the text satisfies the given Expression Language expression. I.e., the line will match if the property value, evaluated as "
                    + "an Expression, returns true. The expression is able to reference FlowFile Attributes, as well as the variables 'line' (which is the text of the line to evaluate) and "
                    + "'lineNo' (which is the line number being evaluated. This will be 1 for the first line, 2 for the second and so on).");

    public static final PropertyDescriptor ATTRIBUTE_TO_UPDATE = new PropertyDescriptor.Builder()
            .name("Attribute to update with classification")
            .description("Specifies the name of the attribute to be updated with the classification tag value.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .defaultValue("classification.tag")
            .dynamic(false)
            .build();

    public static final PropertyDescriptor CLASSIFICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Classification Strategy")
            .description("Determines if the processor should stop after finding a single classification match, or if multiple classifications are supported for a single line.")
            .required(true)
            .allowableValues(CLASSIFY_SINGLE_TAG, CLASSIFY_MULTI_TAG)
            .defaultValue(CLASSIFY_SINGLE_TAG.getValue())
            .dynamic(false)
            .build();

    public static final PropertyDescriptor TRIM_WHITESPACE = new PropertyDescriptor.Builder()
            .name("Ignore Leading/Trailing Whitespace")
            .description("Indicates whether or not the whitespace at the beginning and end of the lines should be ignored when evaluating the line.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .dynamic(false)
            .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the incoming text is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    public static final PropertyDescriptor CLASSIFICATION_MAPPING_FILE = new PropertyDescriptor.Builder()
            .name("Classification Mapping File")
            .description("The name of the file (including the full path) containing the classification mappings. "
                    + "The classification mapping configuration is a JSON Array where each JSON object contains properties "
                    + "for 'classification', 'strategy', 'value' (and optionally 'ignore_case', which defaults to false). \n"
                    + "classification: a tag name to be used when updating the the FlowFile attribute on a classification match. \n"
                    + "strategy: refers to the matching strategy to be used for this classification. "
                    + "Options include 'starts_with', 'ends_with', 'contains', 'equals', 'matches_regex', 'contains_regex', 'satisfies_expression'. \n"
                    + "value: the expression or value to match against (based on which matching strategy is used for this classification).")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .required(true)
            .dynamic(false)
            .build();

    public static final PropertyDescriptor USE_SUBSET_KEY_MAP = new PropertyDescriptor.Builder()
            .name("Limit to subset of classifications based on an attribute key map")
            .description("Indicates whether or not to limit to a subset of classification evaluations based on the value of a specified pre-grouped attribute. ")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .dynamic(false)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_FOR_SUBSET_KEY_MAP = new PropertyDescriptor.Builder()
            .name("Attribute to use for subset classification key mapping")
            .description("Specifies the attribute that will contain a reference key to be referenced in the attribute mapping config file.")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .defaultValue("")
            .dynamic(false)
            .build();

    public static final PropertyDescriptor SUBSET_KEY_MAP_FILE = new PropertyDescriptor.Builder()
            .name("Subset Key Mapping File")
            .description("The name of the file (including the full path) or directory containing the subset classifications key mappings configs. "
                    + "The subset key mapping configuration is a JSON Array where each JSON object contains properties "
                    + "for 'key' and 'classifications'. \n"
                    + "key: a key reference to label each classification subset. This label will be referenced against the specified FlowFile attribute value \n"
                    + "classifications: A JSONArray list of classifications that belong to the specified key. This should match classifications configured in the classification config")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .required(false)
            .dynamic(false)
            .build();


    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input file will be routed to this destination when the lines have been successfully routed to 1 or more relationships")
            .build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("Data that does not satisfy the required user-defined rules will be routed to this Relationship")
            .build();
    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("Data that satisfies the required user-defined rules will be routed to this Relationship")
            .build();

    private static Group EMPTY_GROUP = new Group(Collections.<String>emptyList());

    private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    private List<PropertyDescriptor> properties;

    /**
     * Cache of dynamic properties set during {@link #onScheduled(ProcessContext)} for quick access in
     * {@link #onTrigger(ProcessContext, ProcessSession)}
     */
    private volatile Map<String, ClassificationConfig> propertyMap = new LinkedHashMap<>();
    private volatile Map<String, SubsetKeyMappingConfig> subsetKeyMap = new LinkedHashMap<>();
    private volatile Map<String, Map<String, ClassificationConfig>> subsetPropertyMaps = new HashMap<>();
    //private volatile Pattern groupingRegex = null;

    @VisibleForTesting
    final static int PATTERNS_CACHE_MAXIMUM_ENTRIES = 1024;

    /**
     * LRU cache for the compiled patterns. The size of the cache is determined by the value of
     * {@link #PATTERNS_CACHE_MAXIMUM_ENTRIES}.
     */
    @VisibleForTesting
    private final ConcurrentMap<String, Pattern> patternsCache = CacheBuilder.newBuilder()
            .maximumSize(PATTERNS_CACHE_MAXIMUM_ENTRIES)
            .<String, Pattern>build()
            .asMap();

    private Pattern cachedCompiledPattern(final String regex, final boolean ignoreCase) {
        return patternsCache.computeIfAbsent(regex,
                r -> ignoreCase ? Pattern.compile(r, Pattern.CASE_INSENSITIVE) : Pattern.compile(r));
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> set = new HashSet<>();
        set.add(REL_ORIGINAL);
        set.add(REL_NO_MATCH);
        set.add(REL_MATCH);
        relationships = new AtomicReference<>(set);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTE_TO_UPDATE);
        properties.add(CLASSIFICATION_STRATEGY);
        properties.add(CHARACTER_SET);
        properties.add(TRIM_WHITESPACE);
        properties.add(CLASSIFICATION_MAPPING_FILE);
        properties.add(USE_SUBSET_KEY_MAP);
        properties.add(ATTRIBUTE_FOR_SUBSET_KEY_MAP);
        properties.add(SUBSET_KEY_MAP_FILE);

        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * When this processor is scheduled, update the properties into the map
     * for quick access during each onTrigger call
     *
     * @param context ProcessContext used to retrieve dynamic properties
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        // Get the classification configs from external mapping file
        final String fileName = context.getProperty(CLASSIFICATION_MAPPING_FILE).getValue();
        final File file = new File(fileName);
        if (file.exists() && file.isFile() && file.canRead()) {

            try {
                //read in json config file
                byte[] jsonData = Files.readAllBytes(file.toPath());
                getLogger().info("read file");

                ObjectMapper objectMapper = new ObjectMapper();
                List<ClassificationConfig> clList = Arrays.asList(objectMapper.readValue(jsonData, ClassificationConfig[].class));

                final Map<String, ClassificationConfig> newPropertyMap = new LinkedHashMap<>();
                for (ClassificationConfig clConfig : clList) {
                    getLogger().debug("Adding new property: {}", new Object[]{clConfig});
                    newPropertyMap.put(clConfig.getClassification(), clConfig);
                }

                this.propertyMap = newPropertyMap;

            } catch (Exception e) {
                getLogger().error("ClassificationConfig: Error reading mapping file: {}", new Object[]{e.getMessage()});
            }

        } else {
            getLogger().error("Mapping file does not exist or is not readable: {}", new Object[]{fileName});
        }


        // If using subset mappings, get the external mapping file
        if (context.getProperty(USE_SUBSET_KEY_MAP).asBoolean()) {

            // Get the classification configs from external mapping file
            final String subsetFileName = context.getProperty(SUBSET_KEY_MAP_FILE).getValue();
            final File subsetMappingFile = new File(subsetFileName);

            if (subsetMappingFile.exists() && subsetMappingFile.isFile() && subsetMappingFile.canRead()) {

                try {
                    //read in json config file
                    byte[] jsonData = Files.readAllBytes(subsetMappingFile.toPath());

                    ObjectMapper objectMapper = new ObjectMapper();
                    List<SubsetKeyMappingConfig> clList = Arrays.asList(objectMapper.readValue(jsonData, SubsetKeyMappingConfig[].class));

                    final Map<String, SubsetKeyMappingConfig> newSubsetKeyMap = new LinkedHashMap<>();
                    for (SubsetKeyMappingConfig clConfig : clList) {
                        getLogger().debug("Adding new property: {}", new Object[]{clConfig});
                        newSubsetKeyMap.put(clConfig.getKey(), clConfig);
                    }

                    this.subsetKeyMap = newSubsetKeyMap;

                } catch (Exception e) {
                    getLogger().error("SubsetAttributeMap: Error reading mapping file: {}", new Object[]{e.getMessage()});
                }

            } else if (subsetMappingFile.isDirectory()){
                // if directory path is given, use all json files within directory
                for (final File fileConfig : subsetMappingFile.listFiles()) {
                    getLogger().info("Found File: " + fileConfig + " - Will attempt to use for subset mapping config.");
                    // ignore sub-directories
                    if (fileConfig.isDirectory()) {
                        getLogger().info("Subset attribute mapping file, skipping subdirectory " + fileConfig);
                    } else {

                        try {
                            //read in json config file
                            byte[] jsonData = Files.readAllBytes(fileConfig.toPath());

                            ObjectMapper objectMapper = new ObjectMapper();
                            List<SubsetKeyMappingConfig> clList = Arrays.asList(objectMapper.readValue(jsonData, SubsetKeyMappingConfig[].class));

                            final Map<String, SubsetKeyMappingConfig> newSubsetKeyMap = new LinkedHashMap<>();
                            for (SubsetKeyMappingConfig clConfig : clList) {
                                getLogger().debug("Adding new property: {}", new Object[]{clConfig});
                                newSubsetKeyMap.put(clConfig.getKey(), clConfig);
                            }

                            this.subsetKeyMap.putAll(newSubsetKeyMap);

                        } catch (Exception e) {
                            getLogger().error("SubsetAttributeMapDir: Error reading mapping file: {}", new Object[]{e.getMessage()});
                        }
                    }
                }

            } else {
                getLogger().error("Subset mapping file does not exist or is not readable: {}", new Object[]{fileName});
            }




            // a subset mapping is being used, create a ClassificationsConfig map for each subset group
            final String attributeForSubsetMap = context.getProperty(ATTRIBUTE_FOR_SUBSET_KEY_MAP).getValue();
            final Map<String, SubsetKeyMappingConfig> subsetMap = this.subsetKeyMap;

            for (final Map.Entry<String, SubsetKeyMappingConfig> entry : subsetMap.entrySet()) {

                // each entry in subsetMap is a 'tenant' group
                // find classifications they use
                // create a map of all classification configs for each tenant group
                // add it to tenant subsetPropertyMaps
                List<String> classificationsToUse;
                Map<String, ClassificationConfig> classificationMap = new LinkedHashMap<>();

                try {
                    classificationsToUse = entry.getValue().getClassifications();

                    for (String classification : classificationsToUse) {
                        // Check that the specified mapping exists as a key in the main property map, if not ignore it
                        if (this.propertyMap.containsKey(classification)) {
                            classificationMap.put(classification, this.propertyMap.get(classification));
                        } else {
                            getLogger().warn("The provided subset mapping key: '" + classification + "' does not exist in the "
                                    + "classifications config file. This key will be ignored.");
                        }
                    }
                    subsetPropertyMaps.put(entry.getValue().getKey(), classificationMap);
                } catch (Exception e) {
                    // Catch any errors
                    getLogger().error(e.toString());
                }

            }

        }


    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final boolean trim = context.getProperty(TRIM_WHITESPACE).asBoolean();
        final boolean singleClassification = ((context.getProperty(CLASSIFICATION_STRATEGY).getValue().equals(CLASSIFY_SINGLE_TAG.getValue())));
        final String attributeToUpdate = context.getProperty(ATTRIBUTE_TO_UPDATE).getValue();

        final boolean useSubsetMap = context.getProperty(USE_SUBSET_KEY_MAP).asBoolean();
        final String attributeForSubsetMap = context.getProperty(ATTRIBUTE_FOR_SUBSET_KEY_MAP).getValue();


        // Add object to ClassificationConfig, where the object is the thing that
        // each line is compared against (cached regex, value after applying expression language etc)
        final Map<String, ClassificationConfig> propMap = this.propertyMap;
        final Map<String, ClassificationConfig> finalMap;

        // Limit to subset of evaluations if subset map is provided
        if (useSubsetMap) {
            if (this.subsetPropertyMaps.containsKey(originalFlowFile.getAttribute(attributeForSubsetMap))) {
                finalMap = this.subsetPropertyMaps.get(originalFlowFile.getAttribute(attributeForSubsetMap));
            } else {
                finalMap = null;
            }
        } else {
            finalMap = propMap;
        }

        for (final Map.Entry<String, ClassificationConfig> entry : propMap.entrySet()) {

            final boolean compileRegex = entry.getValue().getStrategy().equals(matchesRegularExpressionValue) || entry.getValue().getStrategy().equals(containsRegularExpressionValue);
            final boolean usePropValue = entry.getValue().getStrategy().equals(satisfiesExpression);

            if (usePropValue)
            {
                // If we are using the 'satisfiesExpression strategy, we want to keep the
                // PropertyValue as the comparison object
                propMap.put(entry.getKey(), entry.getValue());
            } else {
                // For all other strategies we evaluate the attribute expressions to get our comparison object
                final String value = context.newPropertyValue(entry.getValue().getExpressionValue()).evaluateAttributeExpressions(originalFlowFile).getValue();

                if (compileRegex) {
                    // if using a regex strategy, we check the cache for the precompiled regex pattern
                    entry.getValue().setEvalObject(cachedCompiledPattern(value, entry.getValue().getIgnoreCase()));
                } else {
                    // else just set the value
                    entry.getValue().setEvalObject(entry.getValue().getExpressionValue());
                }
                propMap.put(entry.getKey(), entry.getValue());
            }
        }


        final Map<Relationship, Map<Group, FlowFile>> flowFileMap = new HashMap<>();

        session.read(originalFlowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                try (final Reader inReader = new InputStreamReader(in, charset);
                     final NLKBufferedReader reader = new NLKBufferedReader(inReader)) {

                    final Map<String, String> variables = new HashMap<>(2);

                    int lineCount = 0;
                    String line;
                    while ((line = reader.readLine()) != null) {

                        final String matchLine;
                        if (trim) {
                            matchLine = line.trim();
                        } else {
                            // Always trim off the new-line and carriage return characters before evaluating the line.
                            // The NLKBufferedReader maintains these characters so that when we write the line out we can maintain
                            // these characters. However, we don't actually want to match against these characters.
                            final String lineWithoutEndings;
                            final int indexOfCR = line.indexOf("\r");
                            final int indexOfNL = line.indexOf("\n");
                            if (indexOfCR > 0 && indexOfNL > 0) {
                                lineWithoutEndings = line.substring(0, Math.min(indexOfCR, indexOfNL));
                            } else if (indexOfCR > 0) {
                                lineWithoutEndings = line.substring(0, indexOfCR);
                            } else if (indexOfNL > 0) {
                                lineWithoutEndings = line.substring(0, indexOfNL);
                            } else {
                                lineWithoutEndings = line;
                            }

                            matchLine = lineWithoutEndings;
                        }

                        variables.put("line", line);
                        variables.put("lineNo", String.valueOf(++lineCount));

                        // Create attributes map to update
                        final Map<String, String> attributes = new HashMap<>();
                        int multiAttributeIdentifier = 1;

                        // If no classification configs exist then skip processing as result will be unmatched
                        // (This can occur in the event of using a subset mapping but encounter a attribute key that has no subset mapping configured)
                        if (finalMap != null) {
                            // For each ClassificationConfig to evaluate
                            for (final Map.Entry<String, ClassificationConfig> entry : finalMap.entrySet()) {
                                boolean lineMatchesProperty = lineMatches(matchLine, entry.getValue().getEvalObject(), entry.getValue().getStrategy(), entry.getValue().getIgnoreCase(), originalFlowFile, variables);

                                // Match - single classification strategy only
                                if (lineMatchesProperty) {
                                    // route each individual line to each Relationship that matches. This one matches.

                                    if (singleClassification) {
                                        attributes.put(attributeToUpdate, entry.getKey());
                                        // break to avoid calculating things we don't need
                                        break;
                                    } else {
                                        final String attributeKey = new StringBuilder(attributeToUpdate).append(".").append(multiAttributeIdentifier).toString();
                                        multiAttributeIdentifier++;
                                        attributes.put(attributeKey, entry.getKey());
                                    }
                                }
                            }
                        }


                        // Determine relationship route based on if we classified this line
                        final Relationship relationship;

                        if (attributes.isEmpty()) {
                            // no match
                            relationship = REL_NO_MATCH;
                        } else {
                            // we have a match!
                            relationship = REL_MATCH;
                        }

                        // Append line and add classification attributes (& group by classification)
                        List<String> capturingGroupValues = new ArrayList<>(attributes.values());
                        final Group group = getGroup(capturingGroupValues);
                        appendLine(session, flowFileMap, relationship, originalFlowFile, line, charset, group, attributes);

                    }
                }
            }
        });

        for (final Map.Entry<Relationship, Map<Group, FlowFile>> entry : flowFileMap.entrySet()) {
            final Relationship relationship = entry.getKey();
            final Map<Group, FlowFile> groupToFlowFileMap = entry.getValue();

            for (final Map.Entry<Group, FlowFile> flowFileEntry : groupToFlowFileMap.entrySet()) {
                final Group group = flowFileEntry.getKey();
                final FlowFile flowFile = flowFileEntry.getValue();

                final Map<String, String> attributes = new HashMap<>(2);
                attributes.put(ROUTE_ATTRIBUTE_KEY, relationship.getName());
                attributes.put(GROUP_ATTRIBUTE_KEY, StringUtils.join(group.getCapturedValues(), ", "));

                logger.info("Created {} from {}; routing to relationship {}", new Object[]{flowFile, originalFlowFile, relationship.getName()});
                FlowFile updatedFlowFile = session.putAllAttributes(flowFile, attributes);
                session.getProvenanceReporter().route(updatedFlowFile, entry.getKey());
                session.transfer(updatedFlowFile, entry.getKey());
            }
        }

        // now transfer the original flow file
        FlowFile flowFile = originalFlowFile;
        logger.info("Routing {} to {}", new Object[]{flowFile, REL_ORIGINAL});
        session.getProvenanceReporter().route(originalFlowFile, REL_ORIGINAL);
        flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_KEY, REL_ORIGINAL.getName());
        session.transfer(flowFile, REL_ORIGINAL);
    }


    private Group getGroup(final String line, final Pattern groupPattern) {
        if (groupPattern == null) {
            return EMPTY_GROUP;
        } else {
            final Matcher matcher = groupPattern.matcher(line);
            if (matcher.matches()) {
                final List<String> capturingGroupValues = new ArrayList<>(matcher.groupCount());
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    capturingGroupValues.add(matcher.group(i));
                }
                return new Group(capturingGroupValues);
            } else {
                return EMPTY_GROUP;
            }
        }
    }

    private Group getGroup(final List<String> capturingGroupValues) {
        if (capturingGroupValues.isEmpty()){
            return EMPTY_GROUP;
        } else {
            return new Group(capturingGroupValues);
        }
    }

    private void appendLine(final ProcessSession session, final Map<Relationship, Map<Group, FlowFile>> flowFileMap, final Relationship relationship,
                            final FlowFile original, final String line, final Charset charset, final Group group, final Map<String, String> attributes) {

        Map<Group, FlowFile> groupToFlowFileMap = flowFileMap.get(relationship);
        if (groupToFlowFileMap == null) {
            groupToFlowFileMap = new HashMap<>();
            flowFileMap.put(relationship, groupToFlowFileMap);
        }

        FlowFile flowFile = groupToFlowFileMap.get(group);
        if (flowFile == null) {
            flowFile = session.create(original);
        }

        // add classification attributes
        flowFile = session.putAllAttributes(flowFile, attributes);

        flowFile = session.append(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write(line.getBytes(charset));
            }
        });

        groupToFlowFileMap.put(group, flowFile);
    }


    protected static boolean lineMatches(final String line, final Object comparison, final String matchingStrategy, final boolean ignoreCase,
                                         final FlowFile flowFile, final Map<String, String> variables) {
        switch (matchingStrategy) {
            case startsWithValue:
                if (ignoreCase) {
                    return line.toLowerCase().startsWith(((String) comparison).toLowerCase());
                } else {
                    return line.startsWith((String) comparison);
                }
            case endsWithValue:
                if (ignoreCase) {
                    return line.toLowerCase().endsWith(((String) comparison).toLowerCase());
                } else {
                    return line.endsWith((String) comparison);
                }
            case containsValue:
                if (ignoreCase) {
                    return line.toLowerCase().contains(((String) comparison).toLowerCase());
                } else {
                    return line.contains((String) comparison);
                }
            case equalsValue:
                if (ignoreCase) {
                    return line.equalsIgnoreCase((String) comparison);
                } else {
                    return line.equals(comparison);
                }
            case matchesRegularExpressionValue:
                return ((Pattern) comparison).matcher(line).matches();
            case containsRegularExpressionValue:
                return ((Pattern) comparison).matcher(line).find();
            case satisfiesExpression: {
                final PropertyValue booleanProperty = (PropertyValue) comparison;
                return booleanProperty.evaluateAttributeExpressions(flowFile, variables).asBoolean();
            }
        }

        return false;
    }


    private static class Group {
        private final List<String> capturedValues;

        public Group(final List<String> capturedValues) {
            this.capturedValues = capturedValues;
        }

        public List<String> getCapturedValues() {
            return capturedValues;
        }

        @Override
        public String toString() {
            return "Group" + capturedValues;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((capturedValues == null) ? 0 : capturedValues.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            Group other = (Group) obj;
            if (capturedValues == null) {
                if (other.capturedValues != null) {
                    return false;
                }
            } else if (!capturedValues.equals(other.capturedValues)) {
                return false;
            }

            return true;
        }
    }

}

