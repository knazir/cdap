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

var d3 = require('d3');

export function createBucket(array, columnName, columnType) {
  let data;

  switch (columnType) {
    case 'string':
      data = array.map((row) => {
        return row[columnName].length;
      });
      break;
    case 'float':
    case 'int':
      data = array.map((row) => {
        return parseInt(row[columnName], 10);
      });
      break;
    case 'boolean':
      //??????
      break;
  }

  let extent = d3.extent(data);

  let x = d3.scaleLinear()
    .domain(extent);

  let histogram = d3.histogram()
    .domain(x.domain())
    .thresholds(x.ticks(5));

  let histogramData = histogram(data);
  let resultData = histogramData.map((bucket) => bucket.length);
  let labels = histogramData.map((bucket) => {
    return `${bucket.x0} - ${bucket.x1}`;
  });

  return {
    data: resultData,
    labels
  };
}

