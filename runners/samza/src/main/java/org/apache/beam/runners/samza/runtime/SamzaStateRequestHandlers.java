package org.apache.beam.runners.samza.runtime;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.translation.StreamingSideInputHandlerFactory;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

public class SamzaStateRequestHandlers {

  //TODO: support state handlers
  public static StateRequestHandler of(
      ExecutableStage executableStage,
      Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds,
      SideInputHandler sideInputHandler) {
    final StateRequestHandler sideInputStateHandler;
    if (executableStage.getSideInputs().size() > 0) {
      StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
          Preconditions.checkNotNull(
              StreamingSideInputHandlerFactory.forStage(
                  executableStage, sideInputIds, sideInputHandler));
      try {
        sideInputStateHandler =
            StateRequestHandlers.forSideInputHandlerFactory(
                ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize SideInputHandler", e);
      }
    } else {
      sideInputStateHandler = StateRequestHandler.unsupported();
    }

    EnumMap<BeamFnApi.StateKey.TypeCase, StateRequestHandler> handlerMap =
        new EnumMap<>(BeamFnApi.StateKey.TypeCase.class);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.ITERABLE_SIDE_INPUT, sideInputStateHandler);
    return StateRequestHandlers.delegateBasedUponType(handlerMap);
  }
}
