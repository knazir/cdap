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

import React, {Component} from 'react';
import {Provider} from 'react-redux';
require('./SchemaEditor.less');
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';
import {getParsedSchema} from 'components/SchemaEditor/SchemaHelpers';

import SchemaStore from 'components/SchemaEditor/SchemaStore';

export default class SchemaEditor extends Component {
  constructor(props) {
    super(props);
    let state = SchemaStore.getState();
    this.state = {
      rows: getParsedSchema(state.schema)
    };
  }
  render() {
    return (
      <Provider store={SchemaStore}>
        <div className="schema-editor">
          <div className="schema-header">
            <div className="field-name">
              Name
            </div>
            <div className="field-type">
              Type
            </div>
            <div className="field-isnull">
              Null
            </div>
          </div>
          {
            this.state
                .rows
                .map((row, index) => {
                  return (
                    <AbstractSchemaRow
                      key={index}
                      row={row}
                    />
                  );
                })
          }
        </div>
      </Provider>
    );
  }
}
