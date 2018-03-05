/**
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

package client;

import java.io.IOException;

import protocol.protocolWritable.ApplicationSubmissionContext;





import resourceManager.scheduler.ApplicationId;
import service.AbstractService;

public abstract class YarnClient extends AbstractService {

 

  protected YarnClient(String name) {
    super(name);
  }

 

  /**
   * <p>
   * Submit a new application to <code>YARN.</code> It is a blocking call, such
   * that it will not return {@link ApplicationId} until the submitted
   * application has been submitted and accepted by the ResourceManager.
   * </p>
   * 
   * @param appContext
   *          {@link ApplicationSubmissionContext} containing all the details
   *          needed to submit a new application
   * @return {@link ApplicationId} of the accepted application
   * @throws YarnException
   * @throws IOException
   * @see #createApplication()
   */
  public abstract void submitApplication(ApplicationSubmissionContext appContext);

  
  /**
   * <p>
   * Obtain a {@link YarnClientApplication} for a new application,
   * which in turn contains the {@link ApplicationSubmissionContext} and
   * {@link protocol.protocolWritable.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse}
   * objects.
   * </p>
   *
   * @return {@link YarnClientApplication} built for a new application
   * @throws YarnException
   * @throws IOException
   */
  public abstract YarnClientApplication createApplication();
}
