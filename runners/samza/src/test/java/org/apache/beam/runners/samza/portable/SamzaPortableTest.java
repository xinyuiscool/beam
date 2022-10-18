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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.beam.runners.portability.testing.TestPortablePipelineOptions;
import org.apache.beam.runners.portability.testing.TestPortableRunner;
import org.apache.beam.runners.samza.SamzaJobServerDriver;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

@SuppressWarnings({
    "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
    "unused" // TODO(BEAM-13271): Remove when new version of errorprone is released (2.11.0)
})
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

    SamzaPipelineOptions samzaOptions = options.as(SamzaPipelineOptions.class);
    samzaOptions.setMaxBundleSize(10);

    Pipeline pipeline = Pipeline.create(options);
    createStatefulPipeline(pipeline);
    pipeline.run().waitUntilFinish();
  }

  private static void createSimplePipeline(Pipeline pipeline) {
    pipeline
        .apply(Create.of(1, 2, 3, 4))
        .apply(ParDo.of(new DoFn<Integer, Void>() {
          @ProcessElement
          public void process(ProcessContext c) {
            System.out.println(c.element());
          }
        }));
  }

  private static void createStatefulPipeline(Pipeline pipeline) {
    final List<KV<String, Integer>> input = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      input.add(KV.of("" + i, 1));
    }

    final String sumStateId = "count-state";
    final DoFn<KV<String, Integer>, Void> doFn = new DoFn<KV<String, Integer>, Void>() {
      @StateId(sumStateId)
      private final StateSpec<CombiningState<Integer, int[], Integer>> sumState =
          StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

      @ProcessElement
      public void processElement(
          ProcessContext c,
          @StateId(sumStateId) CombiningState<Integer, int[], Integer> count) {

        randomSleep();
        KV<String, Integer> value = c.element();
        count.add(value.getValue());

        System.out.println("thread 2 is " + Thread.currentThread().getName());
        System.out.println("sum is " + count.read());
      }
    };

    pipeline
        .apply(Create.of(input))
        .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
          @ProcessElement
          public void process(ProcessContext c) {
            randomSleep();
            c.output(c.element());
            System.out.println("thread 1 is " + Thread.currentThread().getName());
          }
        }))
        .apply(ParDo.of(doFn));
  }

  private static void randomSleep() {
    Random r = new Random();
    try {
      int s = r.nextInt(10);
      System.out.println("sleep " + s);
      Thread.sleep(s);
    } catch (Exception e) { }
  }
}
