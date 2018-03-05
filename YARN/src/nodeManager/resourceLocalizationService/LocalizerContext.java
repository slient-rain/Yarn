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

package nodeManager.resourceLocalizationService;

import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.ContainerId;



public class LocalizerContext {

	private final String user;
	private final ContainerId containerId;
	private final ApplicationId applicationId;
	//  private final Credentials credentials;

	public LocalizerContext(String user, 
			ContainerId containerId,
			ApplicationId applicationId
			//		  , Credentials credentials
			) {
		this.user = user;
		this.containerId = containerId;
		//    this.credentials = credentials;
		this.applicationId=applicationId;
	}

	public String getUser() {
		return user;
	}

	public ContainerId getContainerId() {
		return containerId;
	}
	public ApplicationId getApplicatioId() {
		return applicationId;
	}

	//  public Credentials getCredentials() {
	//    return credentials;
	//  }

}
