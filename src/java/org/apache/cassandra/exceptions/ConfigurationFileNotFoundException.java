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
package org.apache.cassandra.exceptions;

public class ConfigurationFileNotFoundException extends ConfigurationException
{
    /*
     * If the error is logged should a stack trace be included.
     * For expected errors with an informative message no stack trace needs to be logged.
     * This is just a suggestion to exception handlers as to how they should format the exception.
     */
    public final boolean logStackTrace;
    
    public ConfigurationFileNotFoundException(String msg)
    {
        super(msg);
        logStackTrace = true;
    }

    public ConfigurationFileNotFoundException(String msg, boolean logStackTrace)
    {
        super(msg);
        this.logStackTrace = logStackTrace;
    }

    public ConfigurationFileNotFoundException(String msg, Throwable e)
    {
        super(msg, e);
        logStackTrace = true;
    }

    protected ConfigurationFileNotFoundException(ExceptionCode code, String msg)
    {
        super(code, msg);
        logStackTrace = true;
    }
}
