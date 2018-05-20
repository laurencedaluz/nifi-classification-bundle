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

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

public class ClassifyTextWithMappingTest {

    @Test
    public void testProcessor() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);

        runner.enqueue("start middle end\nnot match\nstart middle end".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart middle end".getBytes("UTF-8"));
        outMatched.assertAttributeEquals("classification.tag", "event_source_1");
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertAttributeNotExists("classification.tag");
        outUnmatched.assertContentEquals("not match\n".getBytes("UTF-8"));
    }

    @Test
    public void testRelationshipOutputs() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);

        runner.enqueue("start middle end\nnot match\nstart middle end".getBytes("UTF-8"));
        runner.run();

        Set<Relationship> relationshipSet = runner.getProcessor().getRelationships();
        Set<String> expectedRelationships = new HashSet<>(Arrays.asList("matched", "unmatched", "original"));

        assertEquals(expectedRelationships.size(), relationshipSet.size());
        for (Relationship relationship : relationshipSet) {
            assertTrue(expectedRelationships.contains(relationship.getName()));
        }

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart middle end".getBytes("UTF-8"));
        outMatched.assertAttributeEquals("classification.tag", "event_source_1");
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertAttributeNotExists("classification.tag");
        outUnmatched.assertContentEquals("not match\n".getBytes("UTF-8"));
        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals("start middle end\nnot match\nstart middle end");


    }

    @Test
    public void testClassificationStrategyNotKnown() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.STARTS_WITH);

        runner.assertNotValid();
    }

    @Test
    public void testNotText() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);


        runner.enqueue(Paths.get("src/test/resources/ClassifyTextWithMappingTest/simple.jpg"));
        runner.run();

        runner.assertTransferCount("matched", 0);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/ClassifyTextWithMappingTest/simple.jpg"));
    }

    @Test
    public void testInvalidRegex() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config_invalid_regex.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        try {
            runner.run();
            fail();
        } catch (AssertionError e) {
            // Expect to catch error asserting 'simple' as invalid
        }

    }

    @Test
    public void testSimpleDefaultStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config_starts_with.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);


        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleCaseSensitiveStartsMatch() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config_starts_with.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);


        runner.enqueue("STart middle end\nstart middle end".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("STart middle end\n".getBytes("UTF-8"));

    }

    @Test
    public void testSimpleCaseInsensitiveStartsMatch() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config_starts_with_ignore_case.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);


        runner.enqueue("STart middle end\nstart middle end".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("STart middle end\nstart middle end".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleDefaultEnd() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config_ends_with.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);


        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testToMultipleClassifications() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_MULTI_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config_multi_match.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);

        runner.enqueue("start middle end\nnot match\nstart middle end".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart middle end".getBytes("UTF-8"));
        outMatched.assertAttributeEquals("classification.tag.1", "event_source_1");
        outMatched.assertAttributeEquals("classification.tag.2", "event_source_2");
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertAttributeNotExists("classification.tag");
        outUnmatched.assertContentEquals("not match\n".getBytes("UTF-8"));
    }

    @Test
    public void testSingleClassificationOrder() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config_multi_match.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);

        runner.enqueue("start middle end\nnot match\nstart middle end".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\nstart middle end".getBytes("UTF-8"));
        outMatched.assertAttributeEquals("classification.tag", "event_source_1");
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertAttributeNotExists("classification.tag");
        outUnmatched.assertContentEquals("not match\n".getBytes("UTF-8"));
    }

    @Test
    public void testWithSubsetMap() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config_multi_match.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);
        runner.setProperty(ClassifyTextWithMapping.USE_SUBSET_KEY_MAP, "true");
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_FOR_SUBSET_KEY_MAP, "tenant");
        final String subsetMappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/MultiTenantSubMappings/").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.SUBSET_KEY_MAP_FILE, subsetMappingFile);

        final Map<String, String> attributes_1 = new HashMap<>();
        attributes_1.put("tenant","tenant_1");
        runner.enqueue("middle end\nnot match\nstart middle end".getBytes("UTF-8"), attributes_1);

        final Map<String, String> attributes_2 = new HashMap<>();
        attributes_2.put("tenant","tenant_2");
        runner.enqueue("middle end\nnot match\nstart middle end".getBytes("UTF-8"), attributes_2);
        runner.run(2);


        runner.assertTransferCount("matched", 2);
        runner.assertTransferCount("unmatched", 2);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched_1 = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched_1.assertContentEquals("start middle end".getBytes("UTF-8"));
        outMatched_1.assertAttributeEquals("tenant", "tenant_1");
        outMatched_1.assertAttributeEquals("classification.tag", "event_source_2");
        final MockFlowFile outMatched_2 = runner.getFlowFilesForRelationship("matched").get(1);
        outMatched_2.assertContentEquals("middle end\nstart middle end".getBytes("UTF-8"));
        outMatched_2.assertAttributeEquals("tenant", "tenant_2");
        outMatched_2.assertAttributeEquals("classification.tag", "event_source_1");

        final MockFlowFile outUnmatched_1 = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched_1.assertAttributeNotExists("classification.tag");
        outUnmatched_1.assertAttributeEquals("tenant", "tenant_1");
        outUnmatched_1.assertContentEquals("middle end\nnot match\n".getBytes("UTF-8"));

        final MockFlowFile outUnmatched_2 = runner.getFlowFilesForRelationship("unmatched").get(1);
        outUnmatched_2.assertAttributeNotExists("classification.tag");
        outUnmatched_2.assertAttributeEquals("tenant", "tenant_2");
        outUnmatched_2.assertContentEquals("not match\n".getBytes("UTF-8"));
    }

    @Test
    public void testWithSubsetMapInvalidConfig() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_TO_UPDATE, "classification.tag");
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_STRATEGY, ClassifyTextWithMapping.CLASSIFY_SINGLE_TAG);
        runner.setProperty(ClassifyTextWithMapping.TRIM_WHITESPACE, "true");
        final String mappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/classifications_config_multi_match.json").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.CLASSIFICATION_MAPPING_FILE, mappingFile);
        runner.setProperty(ClassifyTextWithMapping.USE_SUBSET_KEY_MAP, "true");
        runner.setProperty(ClassifyTextWithMapping.ATTRIBUTE_FOR_SUBSET_KEY_MAP, "tenant");
        final String subsetMappingFile = Paths.get("src/test/resources/ClassifyTextWithMappingTest/MultiTenantSubMappings/").toFile().getAbsolutePath();
        runner.setProperty(ClassifyTextWithMapping.SUBSET_KEY_MAP_FILE, subsetMappingFile);

        final Map<String, String> attributes_1 = new HashMap<>();
        attributes_1.put("tenant","tenant_1");
        runner.enqueue("middle end\nnot match\nstart middle end".getBytes("UTF-8"), attributes_1);

        final Map<String, String> attributes_2 = new HashMap<>();
        attributes_2.put("tenant","tenant_3");
        runner.enqueue("middle end\nnot match\nstart middle end".getBytes("UTF-8"), attributes_2);
        runner.run(2);


        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 2);
        runner.assertTransferCount("original", 2);
        final MockFlowFile outMatched_1 = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched_1.assertContentEquals("start middle end".getBytes("UTF-8"));
        outMatched_1.assertAttributeEquals("tenant", "tenant_1");
        outMatched_1.assertAttributeEquals("classification.tag", "event_source_2");

        final MockFlowFile outUnmatched_1 = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched_1.assertAttributeNotExists("classification.tag");
        outUnmatched_1.assertAttributeEquals("tenant", "tenant_1");
        outUnmatched_1.assertContentEquals("middle end\nnot match\n".getBytes("UTF-8"));

        final MockFlowFile outUnmatched_2 = runner.getFlowFilesForRelationship("unmatched").get(1);
        outUnmatched_2.assertAttributeNotExists("classification.tag");
        outUnmatched_2.assertAttributeEquals("tenant", "tenant_3");
        outUnmatched_2.assertContentEquals("middle end\nnot match\nstart middle end".getBytes("UTF-8"));
    }

/*
    @Test
    public void testGroupSameRelationship() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.CONTAINS);
        runner.setProperty(ClassifyTextWithMapping.GROUPING_REGEX, "(.*?),.*");
        runner.setProperty("o", "o");

        final String originalText = "1,hello\n2,world\n1,good-bye";
        runner.enqueue(originalText.getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("o", 2);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);

        final List<MockFlowFile> list = runner.getFlowFilesForRelationship("o");

        boolean found1 = false;
        boolean found2 = false;

        for (final MockFlowFile mff : list) {
            if (mff.getAttribute(ClassifyTextWithMapping.GROUP_ATTRIBUTE_KEY).equals("1")) {
                mff.assertContentEquals("1,hello\n1,good-bye");
                found1 = true;
            } else {
                mff.assertAttributeEquals(ClassifyTextWithMapping.GROUP_ATTRIBUTE_KEY, "2");
                mff.assertContentEquals("2,world\n");
                found2 = true;
            }
        }

        assertTrue(found1);
        assertTrue(found2);
    }

    @Test
    public void testMultipleGroupsSameRelationship() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.CONTAINS);
        runner.setProperty(ClassifyTextWithMapping.GROUPING_REGEX, "(.*?),(.*?),.*");
        runner.setProperty("o", "o");

        final String originalText = "1,5,hello\n2,5,world\n1,8,good-bye\n1,5,overt";
        runner.enqueue(originalText.getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("o", 3);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);

        final List<MockFlowFile> list = runner.getFlowFilesForRelationship("o");

        boolean found1 = false;
        boolean found2 = false;
        boolean found3 = false;

        for (final MockFlowFile mff : list) {
            if (mff.getAttribute(ClassifyTextWithMapping.GROUP_ATTRIBUTE_KEY).equals("1, 5")) {
                mff.assertContentEquals("1,5,hello\n1,5,overt");
                found1 = true;
            } else if (mff.getAttribute(ClassifyTextWithMapping.GROUP_ATTRIBUTE_KEY).equals("2, 5")) {
                mff.assertContentEquals("2,5,world\n");
                found2 = true;
            } else {
                mff.assertAttributeEquals(ClassifyTextWithMapping.GROUP_ATTRIBUTE_KEY, "1, 8");
                mff.assertContentEquals("1,8,good-bye\n");
                found3 = true;
            }
        }

        assertTrue(found1);
        assertTrue(found2);
        assertTrue(found3);
    }

    @Test
    public void testGroupDifferentRelationships() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.CONTAINS);
        runner.setProperty(ClassifyTextWithMapping.GROUPING_REGEX, "(.*?),.*");
        runner.setProperty("l", "l");

        final String originalText = "1,hello\n2,world\n1,good-bye\n3,ciao";
        runner.enqueue(originalText.getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("l", 2);
        runner.assertTransferCount("unmatched", 2);
        runner.assertTransferCount("original", 1);

        List<MockFlowFile> lFlowFiles = runner.getFlowFilesForRelationship("l");
        boolean found1 = false;
        boolean found2 = false;
        for (final MockFlowFile mff : lFlowFiles) {
            if (mff.getAttribute(ClassifyTextWithMapping.GROUP_ATTRIBUTE_KEY).equals("1")) {
                mff.assertContentEquals("1,hello\n");
                found1 = true;
            } else {
                mff.assertAttributeEquals(ClassifyTextWithMapping.GROUP_ATTRIBUTE_KEY, "2");
                mff.assertContentEquals("2,world\n");
                found2 = true;
            }
        }

        assertTrue(found1);
        assertTrue(found2);

        List<MockFlowFile> unmatchedFlowFiles = runner.getFlowFilesForRelationship("unmatched");
        found1 = false;
        boolean found3 = false;
        for (final MockFlowFile mff : unmatchedFlowFiles) {
            if (mff.getAttribute(ClassifyTextWithMapping.GROUP_ATTRIBUTE_KEY).equals("1")) {
                mff.assertContentEquals("1,good-bye\n");
                found1 = true;
            } else {
                mff.assertAttributeEquals(ClassifyTextWithMapping.GROUP_ATTRIBUTE_KEY, "3");
                mff.assertContentEquals("3,ciao");
                found3 = true;
            }
        }

        assertTrue(found1);
        assertTrue(found3);

    }

    @Test
    public void testSimpleDefaultContains() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.CONTAINS);
        runner.setProperty("simple", "middle");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleContainsIgnoreCase() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.CONTAINS);
        runner.setProperty(ClassifyTextWithMapping.IGNORE_CASE, "true");
        runner.setProperty("simple", "miDDlE");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }


    @Test
    public void testSimpleDefaultEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.EQUALS);
        runner.setProperty("simple", "start middle end");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleDefaultMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty("simple", ".*(mid).*");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleDefaultContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty("simple", "(m.d)");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    /* ------------------------------------------------------ */
/*
    @Test
    public void testSimpleAnyStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.STARTS_WITH);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", "start");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAnyEnds() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.ENDS_WITH);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", "end");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAnyEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.EQUALS);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", "start middle end");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAnyMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", ".*(m.d).*");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAnyContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", "(m.d)");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    /* ------------------------------------------------------ */

    /*
    @Test
    public void testSimpleAllStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.STARTS_WITH);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", "start middle");
        runner.setProperty("second", "star");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAllEnds() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.ENDS_WITH);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", "middle end");
        runner.setProperty("second", "nd");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAllEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.EQUALS);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", "start middle end");
        runner.setProperty("second", "start middle end");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAllMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", ".*(m.d).*");
        runner.setProperty("second", ".*(t.*m).*");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAllContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", "(m.d)");
        runner.setProperty("second", "(t.*m)");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testRouteOnPropertiesStartsWindowsNewLine() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.STARTS_WITH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\r\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\r\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testRouteOnPropertiesStartsJustCarriageReturn() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.STARTS_WITH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\rnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\r".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSatisfiesExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.SATISFIES_EXPRESSION);
        runner.setProperty("empty", "${incomplete expression");
        runner.assertNotValid();

        runner.setProperty("empty", "${line:isEmpty()}");
        runner.setProperty("third-line", "${lineNo:equals(3)}");
        runner.setProperty("second-field-you", "${line:getDelimitedField(2):trim():equals('you')}");
        runner.enqueue("hello\n\ngood-bye, you\n    \t\t\n");
        runner.run();

        runner.assertTransferCount("empty", 1);
        runner.assertTransferCount("third-line", 1);
        runner.assertTransferCount("second-field-you", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);

        runner.getFlowFilesForRelationship("empty").get(0).assertContentEquals("\n    \t\t\n");
        runner.getFlowFilesForRelationship("third-line").get(0).assertContentEquals("good-bye, you\n");
        runner.getFlowFilesForRelationship("second-field-you").get(0).assertContentEquals("good-bye, you\n");
    }

    @Test
    public void testJson() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.STARTS_WITH);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHING_PROPERTY_NAME);
        runner.setProperty("greeting", "\"greeting\"");
        runner.setProperty("address", "\"address\"");

        runner.enqueue(Paths.get("src/test/resources/TestJson/json-sample.json"));
        runner.run();

        runner.assertTransferCount("greeting", 1);
        runner.assertTransferCount("address", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);

        // Verify text is trimmed
        final MockFlowFile outGreeting = runner.getFlowFilesForRelationship("greeting").get(0);
        String outGreetingString = new String(runner.getContentAsByteArray(outGreeting));
        assertEquals(7, countLines(outGreetingString));
        final MockFlowFile outAddress = runner.getFlowFilesForRelationship("address").get(0);
        String outAddressString = new String(runner.getContentAsByteArray(outAddress));
        assertEquals(7, countLines(outAddressString));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        String outUnmatchedString = new String(runner.getContentAsByteArray(outUnmatched));
        assertEquals(400, countLines(outUnmatchedString));

        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/TestJson/json-sample.json"));

    }


    @Test
    public void testXml() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ClassifyTextWithMapping());
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.CONTAINS);
        runner.setProperty(ClassifyTextWithMapping.ROUTE_STRATEGY, ClassifyTextWithMapping.ROUTE_TO_MATCHING_PROPERTY_NAME);
        runner.setProperty("NodeType", "name=\"NodeType\"");
        runner.setProperty("element", "<xs:element");
        runner.setProperty("name", "name=");

        runner.enqueue(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));
        runner.run();

        runner.assertTransferCount("NodeType", 1);
        runner.assertTransferCount("element", 1);
        runner.assertTransferCount("name", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);


        // Verify text is trimmed
        final MockFlowFile outNode = runner.getFlowFilesForRelationship("NodeType").get(0);
        String outNodeString = new String(runner.getContentAsByteArray(outNode));
        assertEquals(1, countLines(outNodeString));
        final MockFlowFile outElement = runner.getFlowFilesForRelationship("element").get(0);
        String outElementString = new String(runner.getContentAsByteArray(outElement));
        assertEquals(4, countLines(outElementString));
        final MockFlowFile outName = runner.getFlowFilesForRelationship("name").get(0);
        String outNameString = new String(runner.getContentAsByteArray(outName));
        assertEquals(7, countLines(outNameString));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        String outUnmatchedString = new String(runner.getContentAsByteArray(outUnmatched));
        assertEquals(26, countLines(outUnmatchedString));

        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));
    }

    @Test
    public void testPatternCache() throws IOException {
        final ClassifyTextWithMapping ClassifyTextWithMapping = new ClassifyTextWithMapping();
        final TestRunner runner = TestRunners.newTestRunner(ClassifyTextWithMapping);
        runner.setProperty(ClassifyTextWithMapping.MATCH_STRATEGY, ClassifyTextWithMapping.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty("simple", ".*(${someValue}).*");

        runner.enqueue("some text", ImmutableMap.of("someValue", "a value"));
        runner.enqueue("some other text", ImmutableMap.of("someValue", "a value"));
        runner.run(2);

        assertEquals("Expected 1 elements in the cache for the patterns, got" +
                ClassifyTextWithMapping.patternsCache.size(), 1, ClassifyTextWithMapping.patternsCache.size());

        for (int i = 0; i < ClassifyTextWithMapping.PATTERNS_CACHE_MAXIMUM_ENTRIES * 2; ++i) {
            String iString = Long.toString(i);
            runner.enqueue("some text with " + iString + "in it",
                    ImmutableMap.of("someValue", iString));
            runner.run();
        }

        assertEquals("Expected " + ClassifyTextWithMapping.PATTERNS_CACHE_MAXIMUM_ENTRIES +
                        " elements in the cache for the patterns, got" + ClassifyTextWithMapping.patternsCache.size(),
                ClassifyTextWithMapping.PATTERNS_CACHE_MAXIMUM_ENTRIES, ClassifyTextWithMapping.patternsCache.size());

        runner.assertTransferCount("simple", ClassifyTextWithMapping.PATTERNS_CACHE_MAXIMUM_ENTRIES * 2);
        runner.assertTransferCount("unmatched", 2);
        runner.assertTransferCount("original", ClassifyTextWithMapping.PATTERNS_CACHE_MAXIMUM_ENTRIES * 2 + 2);

        runner.setProperty(ClassifyTextWithMapping.IGNORE_CASE, "true");
        assertEquals("Pattern cache is not cleared after changing IGNORE_CASE", 0, ClassifyTextWithMapping.patternsCache.size());
    }*/


    public static int countLines(String str) {
        if (str == null || str.isEmpty()) {
            return 0;
        }

        String lineSeparator;

        if(str.contains("\r\n")){
            lineSeparator = "\r\n";
        } else if(str.contains("\n")){
            lineSeparator = "\n";
        } else if(str.contains("\r")){
            lineSeparator = "\r";
        } else {
            return 1;
        }

        int lines = 0;
        int pos = 0;
        while ((pos = str.indexOf(lineSeparator, pos) + 1) != 0) {
            lines++;
        }
        return lines;
    }
}