/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2008-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package org.sonatype.nexus.repository.httpclient;

/**
 * Connection status of a remote repository
 *
 * @since 3.3
 */
public enum RemoteConnectionStatusType
{
  READY("Ready to Connect"),
  AVAILABLE("Remote Available"),
  BLOCKED("Remote Manually Blocked"),
  AUTO_BLOCKED_UNAVAILABLE("Remote Auto Blocked and Unavailable"),
  UNAVAILABLE("Remote Unavailable");

  private final String description;

  RemoteConnectionStatusType(final String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }
}