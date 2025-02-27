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

import { Injectable } from '@angular/core';

export type ColorKey =
  | 'TOTAL'
  | 'RUNNING'
  | 'FAILED'
  | 'FINISHED'
  | 'CANCELED'
  | 'CANCELING'
  | 'CREATED'
  | 'DEPLOYING'
  | 'RECONCILING'
  | 'IN_PROGRESS'
  | 'SCHEDULED'
  | 'COMPLETED'
  | 'RESTARTING'
  | 'PENDING'
  | 'INITIALIZING';

@Injectable({
  providedIn: 'root'
})
export class ConfigService {
  BASE_URL = '.';

  COLOR_MAP: Record<ColorKey, string> = {
    TOTAL: '#112641',
    RUNNING: '#52c41a',
    FAILED: '#f5222d',
    FINISHED: '#1890ff',
    CANCELED: '#fa8c16',
    CANCELING: '#faad14',
    CREATED: '#2f54eb',
    DEPLOYING: '#13c2c2',
    RECONCILING: '#eb2f96',
    IN_PROGRESS: '#faad14',
    SCHEDULED: '#722ed1',
    COMPLETED: '#1890ff',
    RESTARTING: '#13c2c2',
    INITIALIZING: '#738df8',
    PENDING: '#95a5a6'
  };

  LONG_MIN_VALUE = -9223372036854776000;
}
