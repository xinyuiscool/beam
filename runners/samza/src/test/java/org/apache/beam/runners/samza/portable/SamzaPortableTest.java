/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.samza.portable;

import org.apache.beam.runners.portability.testing.TestPortablePipelineOptions;
import org.apache.beam.runners.portability.testing.TestPortableRunner;
import org.apache.beam.runners.samza.SamzaJobServerDriver;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Test;

public class SamzaPortableTest {

  @Test
  public void test() {
    TestPortablePipelineOptions options =
        PipelineOptionsFactory.as(TestPortablePipelineOptions.class);
    options.setJobServerDriver((Class) SamzaJobServerDriver.class);
    options.setJobServerConfig("--job-host=localhost", "--job-port=0", "--artifact-port=0", "--expansion-port=0");
    options.setRunner(TestPortableRunner.class);
    options.setEnvironmentExpirationMillis(10000);
    options.setDefaultEnvironmentType("EMBEDDED");
    ExperimentalOptions.addExperiment(options.as(ExperimentalOptions.class), "beam_fn_api");

    Pipeline pipeline = createPipeline(options);
    pipeline.run().waitUntilFinish();
  }

  private static Pipeline createPipeline(PipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(Create.of(1, 2, 3, 4))
        .apply(ParDo.of(new DoFn<Integer, Void>() {
          @ProcessElement
          public void process(ProcessContext c) {
            System.out.println(c.element());
          }
        }));
    return pipeline;
  }
}
