/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.customaction.CustomAction;
import co.cask.cdap.api.customaction.CustomActionContext;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.customaction.BasicCustomActionContext;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute the custom action in the Workflow.
 */
class CustomActionExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CustomActionExecutor.class);
  private final ProgramRunId workflowRunId;
  private final BasicWorkflowContext workflowContext;
  private final WorkflowAction action;
  private final CustomAction customAction;
  private final BasicCustomActionContext customActionContext;

  /**
   * Creates instance which will be used to initialize, run, and destroy the custom action.
   * @param workflowRunId the Workflow run which started the execution of this custom action.
   * @param workflowContext an instance of context
   * @param instantiator to instantiates the custom action class
   * @param classLoader used to load the custom action class
   * @throws Exception when failed to instantiate the custom action
   * @deprecated Deprecated as of 3.5.0
   */
  @Deprecated
  CustomActionExecutor(ProgramRunId workflowRunId, BasicWorkflowContext workflowContext,
                       InstantiatorFactory instantiator, ClassLoader classLoader) throws Exception {
    this.workflowRunId = workflowRunId;
    this.workflowContext = workflowContext;
    this.action = createAction(workflowContext, instantiator, classLoader);
    this.customAction = null;
    this.customActionContext = null;
  }

  /**
   * Creates instance which will be used to initialize, run, and destroy the custom action.
   * @param workflowRunId the Workflow run which started the execution of this custom action.
   * @param customActionContext an instance of context
   * @param instantiator to instantiates the custom action class
   * @param classLoader used to load the custom action class
   * @throws Exception when failed to instantiate the custom action
   */
  CustomActionExecutor(ProgramRunId workflowRunId, BasicCustomActionContext customActionContext,
                       InstantiatorFactory instantiator, ClassLoader classLoader) throws Exception {
    this.workflowRunId = workflowRunId;
    this.customActionContext = customActionContext;
    this.customAction = createCustomAction(customActionContext, instantiator, classLoader);
    this.action = null;
    this.workflowContext = null;
  }

  @SuppressWarnings("unchecked")
  @Deprecated
  private WorkflowAction createAction(BasicWorkflowContext context, InstantiatorFactory instantiator,
                                      ClassLoader classLoader) throws Exception {
    Class<?> clz = Class.forName(context.getSpecification().getClassName(), true, classLoader);
    Preconditions.checkArgument(WorkflowAction.class.isAssignableFrom(clz), "%s is not a WorkflowAction.", clz);
    WorkflowAction action = instantiator.get(TypeToken.of((Class<? extends WorkflowAction>) clz)).create();
    Metrics metrics = new ProgramUserMetrics(
      context.getProgramMetrics().childContext(Constants.Metrics.Tag.NODE, context.getSpecification().getName()));
    Reflections.visit(action, action.getClass(),
                      new PropertyFieldSetter(context.getSpecification().getProperties()),
                      new DataSetFieldSetter(context),
                      new MetricsFieldSetter(metrics));
    return action;
  }

  @SuppressWarnings("unchecked")
  private CustomAction createCustomAction(BasicCustomActionContext context, InstantiatorFactory instantiator,
                                          ClassLoader classLoader) throws Exception {
    Class<?> clz = Class.forName(context.getSpecification().getClassName(), true, classLoader);
    Preconditions.checkArgument(CustomAction.class.isAssignableFrom(clz), "%s is not a CustomAction.", clz);
    CustomAction action = instantiator.get(TypeToken.of((Class<? extends CustomAction>) clz)).create();
    Reflections.visit(action, action.getClass(),
                      new PropertyFieldSetter(context.getSpecification().getProperties()),
                      new DataSetFieldSetter(context),
                      new MetricsFieldSetter(context.getMetrics()));
    return action;
  }

  void execute() throws Exception {
    if (action == null) {
      // Execute the new CustomAction
      executeCustomAction();
      return;
    }
    ClassLoader oldClassLoader = setContextCombinedClassLoader(action.getClass().getClassLoader());
    try {
      workflowContext.setState(new ProgramState(ProgramStatus.INITIALIZING, null));
      initializeInTransaction();
      runInTransaction();
      workflowContext.setState(new ProgramState(ProgramStatus.COMPLETED, null));
    } catch (Throwable t) {
      Throwable rootCause = Throwables.getRootCause(t);
      if (rootCause instanceof InterruptedException) {
        workflowContext.setState(new ProgramState(ProgramStatus.KILLED, rootCause.getMessage()));
      } else {
        workflowContext.setState(new ProgramState(ProgramStatus.FAILED, rootCause.getMessage()));
      }
      throw Throwables.propagate(rootCause);
    } finally {
      destroyInTransaction();
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  private void executeCustomAction() throws Exception {
    ClassLoader oldClassLoader = setContextCombinedClassLoader(customAction.getClass().getClassLoader());
    try {
      customActionContext.setState(new ProgramState(ProgramStatus.INITIALIZING, null));
      initialize();
      customActionContext.setState(new ProgramState(ProgramStatus.RUNNING, null));
      customAction.run();
      customActionContext.setState(new ProgramState(ProgramStatus.COMPLETED, null));
    } catch (Throwable t) {
      customActionContext.setState(new ProgramState(ProgramStatus.FAILED, Throwables.getRootCause(t).getMessage()));
      Throwables.propagateIfPossible(t, Exception.class);
      throw Throwables.propagate(t);
    } finally {
      destroy();
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  private void initialize() throws Exception {
    // AbstractCustomAction implements final initialize(context) and requires subclass to
    // implement initialize(), whereas programs that directly implement CustomAction can
    // override initialize(context)
    TransactionControl txControl = customAction instanceof AbstractCustomAction
      ? Transactions.getTransactionControl(TransactionControl.IMPLICIT, AbstractCustomAction.class,
                                           customAction, "initialize")
      : Transactions.getTransactionControl(TransactionControl.IMPLICIT, CustomAction.class,
                                           customAction, "initialize", CustomActionContext.class);
    if (TransactionControl.IMPLICIT == txControl) {
      customActionContext.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          customAction.initialize(customActionContext);
        }
      });
    } else {
      customAction.initialize(customActionContext);
    }
  }

  private void destroy() throws Exception {
    TransactionControl txControl =
      Transactions.getTransactionControl(TransactionControl.IMPLICIT, CustomAction.class, customAction, "destroy");
    try {
      if (TransactionControl.IMPLICIT == txControl) {
        customActionContext.execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            customAction.destroy();
          }
        });
      } else {
        customAction.destroy();
      }
    } catch (Throwable t) {
      LOG.error("Failed to execute the destroy method on action {} for Workflow run {}",
                customActionContext.getSpecification().getName(), workflowRunId, t);
    }
  }

  @Deprecated
  private void initializeInTransaction() throws Exception {
    TransactionContext txContext = workflowContext.getDatasetCache().newTransactionContext();
    txContext.start();
    try {
      action.initialize(workflowContext);
      txContext.finish();
    } catch (TransactionFailureException e) {
      txContext.abort(e);
    } catch (Throwable t) {
      txContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", t));
    }
  }

  @Deprecated
  private void runInTransaction() throws Exception {
    TransactionContext txContext = workflowContext.getDatasetCache().newTransactionContext();
    txContext.start();
    try {
      action.run();
      txContext.finish();
    } catch (TransactionFailureException e) {
      txContext.abort(e);
    } catch (Throwable t) {
      txContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", t));
    }
  }

  @Deprecated
  private void destroyInTransaction() {
    try {
      TransactionContext txContext = workflowContext.getDatasetCache().newTransactionContext();
      txContext.start();
      try {
        action.destroy();
        txContext.finish();
      } catch (TransactionFailureException e) {
        txContext.abort(e);
      } catch (Throwable t) {
        txContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", t));
      }
    } catch (Throwable t) {
      LOG.error("Failed to execute the destroy method on action {} for Workflow run {}",
                workflowContext.getSpecification().getName(), workflowRunId, t);
    }
  }

  private ClassLoader setContextCombinedClassLoader(ClassLoader classLoader) {
    return ClassLoaders.setContextClassLoader(
      new CombineClassLoader(null, ImmutableList.of(classLoader, getClass().getClassLoader())));
  }
}
